"""
Use:

async with kernel_sidecar.Kernel(connection_info) as kernel:
    action = kernel.kernel_info_request()
    await action

print(action.content)
"""

import asyncio
import logging
from typing import List, Optional

import pydantic
import zmq
from jupyter_client import AsyncKernelClient, KernelConnectionInfo
from jupyter_client.channels import ZMQSocketChannel
from zmq.utils.monitor import recv_monitor_message

from kernel_sidecar import actions
from kernel_sidecar.models import messages, requests

logger = logging.getLogger(__name__)


class SidecarKernelClient:
    """
    Primary interface between our Sidecar and a Kernel.
     - Manages the zmq connections between Sidecar and Kernel
     - Sends Pydantic-modeled (`models.requests.py`) messages to the Kernel
     - Watches ZMQ messages for messages coming in from the Kernel
     - Parses messages coming from the Kernel into (`models.messages.py`) Pydantic models
     - Delegates handling those messages to the appropriate Action that started the request-reply
       sequence, and that Action should have Handlers / async fn callbacks to handle business logic
    """

    _message_model = messages.Message
    # ^^ discriminator model to parse messages coming in from ZMQ. Override this if you have
    # custom message models defined somewhere besides kernel_sidecar.models.messages
    # should be type: Annotated[Union[models...], Field(discriminator='msg_type')]
    _handler_timeout: float = None  # optional timeout when awaiting Action handlers

    def __init__(self, connection_info: KernelConnectionInfo):
        self.kc = AsyncKernelClient()
        self.kc.load_connection_info(connection_info)

        self.status: messages.KernelStatus = None
        self.actions: dict[str, actions.KernelAction] = {}

        self.kc.start_channels()

        # message queue, raw data (dict) from all zmq channels gets dropped into here
        # and a separate asyncio.Task picks them up off the queue to pass into the
        # right Action for handling callbacks
        self.mq = asyncio.Queue()

        # Keep track of tasks to cancel while shutting down
        self.mq_task: asyncio.Task = None
        self.channel_watching_tasks: List[asyncio.Task] = []
        self.channel_watcher_parent_tasks: List[asyncio.Task] = []

    def send(self, action: actions.KernelAction) -> actions.KernelAction:
        if action.sent:
            raise RuntimeError(f"{action} already sent to Kernel")

        if action.msg_id in self.actions:
            raise RuntimeError(f"Already tracking reply routing for {action.msg_id=}")

        # Update the .actions dictionary so that we route any observed messages coming to us
        # over ZMQ into this action for handling callbacks
        self.actions[action.msg_id] = action

        # Send the request over the appropriate zmq channel
        try:
            channel: ZMQSocketChannel = getattr(self.kc, f"{action.request._channel}_channel")
            channel.send(action.request.dict())
            action.sent = True
            logger.debug("Sent request to kernel", extra={"body": action.request.dict()})
        except Exception as e:
            logger.exception("Error sending message", extra={"body": action.request.dict()})
            raise e
        return action

    def kernel_info_request(
        self, handlers: List[actions.HandlerType] = None
    ) -> actions.KernelAction:
        req = requests.KernelInfoRequest()
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def execute_request(
        self, code: str, silent: bool = False, handlers: List[actions.HandlerType] = None
    ) -> actions.KernelAction:
        req = requests.ExecuteRequest(content={"code": code, "silent": silent})
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def complete_request(
        self,
        code: str,
        cursor_pos: Optional[int] = None,
        handlers: List[actions.HandlerType] = None,
    ) -> actions.KernelAction:
        if cursor_pos is None:
            cursor_pos = len(code)
        req = requests.CompleteRequest(content={"code": code, "cursor_pos": cursor_pos})
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def interrupt_request(self, handlers: List[actions.HandlerType] = None) -> actions.KernelAction:
        req = requests.InterruptRequest()
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def comm_open_request(
        self,
        target_name: str,
        data: Optional[dict] = None,
        handlers: List[actions.HandlerType] = None,
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
        handlers: List[actions.HandlerType] = None,
    ) -> actions.KernelAction:
        req = requests.CommMsg(content={"comm_id": comm_id, "data": data})
        action = actions.KernelAction(request=req, handlers=handlers)
        return self.send(action)

    def comm_close_request(
        self, comm_id: str, handlers: List[actions.HandlerType] = None
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

        message_task = asyncio.create_task(self._watch_channel_for_messages(channel))
        status_task = asyncio.create_task(
            self._watch_channel_for_status(channel.socket.get_monitor_socket())
        )
        self.channel_watching_tasks.append(message_task)
        self.channel_watching_tasks.append(status_task)

        done, pending = await asyncio.wait(
            [message_task, status_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
        for task in done:
            if task.exception():
                raise task.exception()

        logger.debug("Cycling channel based on task ending", extra={"channel": channel_name})
        self.channel_watching_tasks.remove(message_task)
        self.channel_watching_tasks.remove(status_task)

        # The .<channel_name>_channel properties check if ._<channel_name>_channel attribute
        # is None or not. If it is, it starts the connection on that channel. Setting this attr
        # back to None will force a reconnect next time the property is accessed.
        setattr(self.kc, f"_{channel_name}_channel", None)
        return await self.watch_channel(channel_name)

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

    async def _watch_channel_for_messages(self, channel: ZMQSocketChannel):
        """Takes messages seen on zmq and drops them into our internal asyncio.Queue"""
        while True:
            try:
                if not channel.is_alive():
                    await asyncio.sleep(0.001)
                    continue
                raw_msg: dict = await channel.get_msg()
                logger.debug("Message received on zmq", extra={"body": raw_msg})
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

    async def __aenter__(self) -> "SidecarKernelClient":
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
