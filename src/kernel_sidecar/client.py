"""
The KernelSidecarClient manages sending and receiving messages over zmq, and keeping track of
"Actions" that it uses to delegate received messages to appropriate handlers.

Use:

from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.handlers import DebugHandler

async with KernelSidecarClient(connection_info) as client:
    handler = DebugHandler()
    action = kernel.kernel_info_request(handlers=[handler]))
    await action

assert handler.counts == {"status": 2, "kernel_info_reply": 1}
"""

import asyncio
import logging
from typing import Awaitable, Callable, List, Optional, Type

import pydantic
import zmq
from jupyter_client import AsyncKernelClient, KernelConnectionInfo
from jupyter_client.channels import ZMQSocketChannel
from zmq.asyncio import Context
from zmq.utils.monitor import recv_monitor_message

from kernel_sidecar import actions
from kernel_sidecar.comms import CommHandler, CommManager, CommOpenHandler, CommTargetNotFound
from kernel_sidecar.handlers import Handler
from kernel_sidecar.models import messages, requests

logger = logging.getLogger(__name__)


class KernelSidecarClient:
    """
    Primary interface between our Sidecar and a Kernel.
     - Manages the zmq connections between Sidecar and Kernel
     - Sends Pydantic-modeled (`models.requests.py`) messages to the Kernel
     - Watches ZMQ messages for messages coming in from the Kernel
     - Parses messages coming from the Kernel into (`models.messages.py`) Pydantic models
     - Delegates handling those messages to the appropriate Action that started the request-reply
       sequence, and that Action should have Handlers / async fn callbacks to handle business logic
     - Includes options for "default handlers" applied to all Actions, and a reference to the
       CommManager, which will process and/or route comm_open, comm_msg, and comm_close messages
    """

    _message_model = messages.Message
    # ^^ discriminator model to parse messages coming in from ZMQ. Override this if you have
    # custom message models defined somewhere besides kernel_sidecar.models.messages
    # should be type: Annotated[Union[models...], Field(discriminator='msg_type')]
    _handler_timeout: float = None  # optional timeout when awaiting Action handlers

    def __init__(
        self,
        connection_info: KernelConnectionInfo,
        max_message_size: Optional[int] = None,
        default_handlers: List[Handler] = None,
        comm_manager: Optional[CommManager] = None,
    ):
        """
        - connection_info: dict with zmq ports the Kernel has open
        - max_message_size: optional setting applied to zmq socket configuration so that we'll
          automatically close the socket if the message is over that size. Useful if the sidecar
          application has less memory than the Kernel and need to avoid OOM in sidecar from large
          outputs or other messages coming in from the Kernel
        - default_handlers: appended to every Action's handler list when Action request is sent
        """
        zmq_context = Context()  # only relevant if max_message_size is set
        if max_message_size:
            zmq_context.setsockopt(zmq.SocketOption.MAXMSGSIZE, max_message_size)
        self.kc = AsyncKernelClient(context=zmq_context)
        self.kc.load_connection_info(connection_info)
        self.kc.start_channels()

        # Used to delegate received messages to handlers attached to the Action
        # When we send a request to the kernel, we use the request msg_id {msg_id: Action}
        # When we receive messages from kernel, we look up the Action by the parent_header.msg_id
        # and delegate the messages to handlers attached to that Action
        self.actions: dict[str, actions.KernelAction] = {}

        # message queue, raw data (dict) from all zmq channels gets dropped into here
        # and a separate asyncio.Task picks them up off the queue to pass into the
        # right Action for handling callbacks
        self.mq = asyncio.Queue()

        # Keep track of tasks to cancel while shutting down
        self.mq_task: asyncio.Task = None
        self.channel_watching_tasks: List[asyncio.Task] = []
        self.channel_watcher_parent_tasks: List[asyncio.Task] = []

        # Handlers to attach to every Action. These will be appended to action.handlers
        # during .send, which means they'll run /after/ other handlers.
        # Fail right away if there's common mistake of init'ing with default_handlers=Handler()
        if default_handlers and not isinstance(default_handlers, list):
            raise RuntimeError(f"{default_handlers=} must be None or a list")
        self.default_handlers = default_handlers or []

        self.comm_manager = comm_manager or CommManager()

    @property
    def running_action(self):
        """
        Return a best guess at what action the Kernel is handling right now.

        The logic here is that actions is a dict, which is ordered in Python 3.6+, so iterate
        through them until we find an action that is not done yet. The first non-done action is
        the one we're looking for.
        """
        for action in self.actions.values():
            if not action.done.is_set():
                return action

    def send(self, action: actions.KernelAction) -> actions.KernelAction:
        if action.sent:
            raise RuntimeError(f"{action} already sent to Kernel")

        if action.msg_id in self.actions:
            raise RuntimeError(f"Already tracking reply routing for {action.msg_id=}")

        # Reminder: when messages are processed, they get sent to each Action handler in order so
        # default handlers will run /after/ handlers already attached to this Action and the
        # CommManager handler will run last
        action.handlers.extend(self.default_handlers)
        action.handlers.append(self.comm_manager)

        # Send the request over the appropriate zmq channel
        try:
            channel: ZMQSocketChannel = getattr(self.kc, f"{action.request._channel}_channel")
            channel.send(action.request.dict())
            action.sent = True
            # Update the .actions dictionary so that we route any observed messages coming to us
            # over ZMQ into this action for handling callbacks
            self.actions[action.msg_id] = action
            logger.debug("Sent request to kernel", extra={"body": action.request.dict()})
        except Exception as e:
            logger.exception("Error sending message", extra={"body": action.request.dict()})
            raise e
        return action

    def kernel_info_request(
        self, handlers: List[Callable[[messages.Message], Awaitable[None]]] = None
    ) -> actions.KernelAction:
        req = requests.KernelInfoRequest()
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def execute_request(
        self, code: str, silent: bool = False, handlers: List[Handler] = None
    ) -> actions.KernelAction:
        req = requests.ExecuteRequest(content={"code": code, "silent": silent})
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def complete_request(
        self,
        code: str,
        cursor_pos: Optional[int] = None,
        handlers: List[Handler] = None,
    ) -> actions.KernelAction:
        if cursor_pos is None:
            cursor_pos = len(code)
        req = requests.CompleteRequest(content={"code": code, "cursor_pos": cursor_pos})
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def interrupt_request(self, handlers: List[Handler] = None) -> actions.KernelAction:
        req = requests.InterruptRequest()
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    async def comm_open(
        self, target_name: str, handler_cls: Type[CommHandler], data: Optional[dict] = None
    ) -> CommHandler:
        """
        High level helper for opening a comm from the sidecar side. If there's no comm handling
        function registered on the kernel for the given target_name, the kernel will send out
        a stream message with stderr and a comm_close event, which we'll raise here.
        """
        # think of this handler as an ephemeral handler just here so we can raise an error if the
        # kernel reports that the comm target is not found
        msg_handler = CommOpenHandler()
        # Create Action and send comm_open over zmq
        action = self.comm_open_request(target_name, data, handlers=[msg_handler])
        # pull out comm_id from the request
        req: requests.CommOpen = action.request
        comm_id = req.content.comm_id
        # Register the CommHandler in the CommManager before awaiting the Action, so CommManager
        # can observe any comm_msg during the open process and delegate to CommHandler.
        # If there's an error, comm_manager will deregister while handling comm_close msg
        comm_handler = handler_cls(comm_id=comm_id)
        self.comm_manager.comms[comm_id] = comm_handler
        logger.debug("registered comm", extra={"comm_id": comm_id, "handler": comm_handler})
        await action
        if msg_handler.comm_closed_id == comm_id:
            raise CommTargetNotFound(msg_handler.comm_err_msg)
        return comm_handler

    def comm_open_request(
        self,
        target_name: str,
        data: Optional[dict] = None,
        handlers: List[Handler] = None,
    ) -> actions.KernelAction:
        if data is None:
            data = {}
        req = requests.CommOpen(content={"target_name": target_name, "data": data})
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def comm_msg_request(
        self,
        comm_id: str,
        data: Optional[dict] = None,
        handlers: List[Handler] = None,
    ) -> actions.KernelAction:
        req = requests.CommMsg(content={"comm_id": comm_id, "data": data})
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def comm_close_request(
        self, comm_id: str, handlers: List[Handler] = None
    ) -> actions.KernelAction:
        req = requests.CommClose(content={"comm_id": comm_id})
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def send_stdin(self, value: str) -> None:
        """
        Send content over stdin chanenl.

        If an execute_request includes input() in its source, the Kernel will emit an input_request
        and expect to see an input_reply come back from sidecar -> kernel over stdin channel before
        continuing with other shell channel handling (e.g. more execute requests).

        This is not wrapped as an Action because there are no replies for input_reply (if anything
        the Sidecar is replying to the Kernel's input_request)
        """
        req = requests.InputReply(content={"value": value})
        try:
            self.kc.stdin_channel.send(req.dict())
        except Exception:
            logger.exception("Error sending input_reply to stdin", extra={"body": req.dict()})

    async def watch_channel(self, channel_name: str):
        """
        Watch a specific ZMQ channel, picking up messages coming in from the kernel and dropping
        those onto the internal asyncio.Queue for processing (delegating to Action handlers).

        Cycles the ZMQ connection if it's lost.
        """
        logger.debug("Channel watcher started", extra={"channel": channel_name})
        channel: ZMQSocketChannel = getattr(self.kc, f"{channel_name}_channel")

        message_task = asyncio.create_task(self._watch_channel_for_messages(channel, channel_name))
        status_task = asyncio.create_task(
            self._watch_channel_for_status(channel.socket.get_monitor_socket())
        )
        self.channel_watching_tasks.append(message_task)
        self.channel_watching_tasks.append(status_task)

        done, pending = await asyncio.wait(
            [message_task, status_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        logger.debug("Cycling channel based on task ending", extra={"channel": channel_name})

        # Reconnect ASAP
        # The .<channel_name>_channel properties check if ._<channel_name>_channel attribute
        # is None or not. If it is, it starts the connection on that channel. Setting this attr
        # back to None will force a reconnect next time the property is accessed.
        setattr(self.kc, f"_{channel_name}_channel", None)
        task = asyncio.create_task(self.watch_channel(channel_name))
        self.channel_watcher_parent_tasks.append(task)

        # Finish cleanup
        for task in pending:
            task.cancel()
        for task in done:
            if task.exception():
                raise task.exception()

        self.channel_watching_tasks.remove(message_task)
        self.channel_watching_tasks.remove(status_task)

        # Provide a hook for subclasses to take action on channel disconnects
        await self.handle_zmq_disconnect(channel_name)

    async def _watch_channel_for_status(self, monitor_socket: zmq.Socket):
        """
        Watches for zmq channel disconnects and returns so that the higher level
        watch_channel coroutine will "cycle" this connection.

        The main use-case here is when the zmq socket context is configured with a max message
        size to avoid OOM'ing the sidecar from massive outputs. When that happens, the zmq socket
        is closed but not automatically opened again.

        If you're not using a max message size, this task and the complementary message-watching
        task should never break out of their loops unless the kernel dies.
        """
        while True:
            try:
                msg: dict = await recv_monitor_message(monitor_socket)
                event: zmq.Event = msg["event"]
                if event == zmq.EVENT_DISCONNECTED:
                    return
            except asyncio.CancelledError:
                break

    async def _watch_channel_for_messages(self, channel: ZMQSocketChannel, channel_name: str):
        """Takes messages seen on zmq and drops them into our internal asyncio.Queue"""
        while True:
            try:
                if not channel.is_alive():
                    await asyncio.sleep(0.001)
                    continue
                raw_msg: dict = await channel.get_msg()
                msg_type = raw_msg.get("msg_type", "")
                logger.debug(
                    f"Message {msg_type} on {channel_name}",
                    extra={"body": raw_msg, "channel": channel_name},
                )
                self.mq.put_nowait(raw_msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error retrieving message from zmq")
                raise e

    async def process_message(self):
        """
        Takes messages from our internal asyncio.Queue, parses them into pydantic models using
        a discriminator pattern (see messages.py), and delegates them to the appropriate Action
        which will further delegate them to handle_<msg_type> methods defined in the Action model.

        Action handlers are awaited before the next message is processed.
        """
        while True:
            try:
                raw_msg: dict = await self.mq.get()
                if not raw_msg.get("parent_header"):
                    await self.handle_missing_parent_msg_id(raw_msg)
                    continue

                msg = pydantic.parse_obj_as(messages.Message, raw_msg)
                if msg.parent_header.msg_id not in self.actions:
                    await self.handle_untracked_action(msg)
                    continue

                action = self.actions[msg.parent_header.msg_id]
                await asyncio.wait_for(action.handle_message(msg), timeout=self._handler_timeout)

            except pydantic.ValidationError as e:
                await self.handle_unparseable_message(raw_msg, e)
            except asyncio.CancelledError:
                logger.debug("Message processing Task cancelled")
                break
            except Exception as e:
                logger.exception("Error while handling message")
                raise e

    async def handle_missing_parent_msg_id(self, raw_msg: dict):
        """
        Almost all messages should have a parent_header.msg_id which we can use to delegate to
        Action handlers. Any messages without a parent_header msg_id can be logged or handled
        here.

        One notable example of a message with no parent header is the kernel status for execution
        state: "starting". If your app is tracking Kernel State, you probably want to override
        this method in a subclass to set "starting" state.
        """
        pass

    async def handle_untracked_action(self, msg: messages.Message):
        """
        In theory if we're the only client talking to a kernel, we shouldn't get into this method.
        Override in subclasses for logging or handling of untracked messages.
        """
        pass

    async def handle_unparseable_message(self, raw_msg: dict, error: pydantic.ValidationError):
        """
        Override in subclasses for logging or handling of unparseable messages.
        """
        pass

    async def handle_zmq_disconnect(self, channel_name: str):
        pass

    async def __aenter__(self) -> "KernelSidecarClient":
        # General asyncio comment: make sure tasks always have a reference (assigned to variable or
        # being awaited) or they might be garbage collected while running.
        for channel in ["iopub", "shell", "control", "stdin"]:
            task = asyncio.create_task(self.watch_channel(channel))
            self.channel_watcher_parent_tasks.append(task)
        self.mq_task = asyncio.create_task(self.process_message())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Exiting the async context / general cleanup consists of:
        # - cancel all tasks
        # - stop zmq channel connections
        for task in self.channel_watcher_parent_tasks:
            task.cancel()
        for task in self.channel_watching_tasks:
            task.cancel()
        if self.mq_task:
            self.mq_task.cancel()
        self.kc.stop_channels()
        self.kc.stop_channels()
