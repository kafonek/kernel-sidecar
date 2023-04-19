"""
The KernelSidecarClient manages sending and receiving messages over zmq, and keeping track of
"Actions" that it uses to delegate received messages to appropriate handlers.

Use:

from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.handlers.debug import DebugHandler

async with KernelSidecarClient(connection_info) as client:
    handler = DebugHandler()
    action = kernel.kernel_info_request(handlers=[handler]))
    await action

assert handler.counts == {"status": 2, "kernel_info_reply": 1}
"""

import asyncio
import logging
import pprint
from typing import Awaitable, Callable, List, Optional, Type

import pydantic
import zmq
from jupyter_client import AsyncKernelClient, KernelConnectionInfo
from jupyter_client.channels import ZMQSocketChannel
from zmq.asyncio import Context
from zmq.utils.monitor import recv_monitor_message

from kernel_sidecar import actions
from kernel_sidecar.comms import CommHandler, CommManager, WidgetHandler
from kernel_sidecar.handlers.base import Handler
from kernel_sidecar.models import messages, requests
from kernel_sidecar.settings import get_settings

logger = logging.getLogger(__name__)


class CommTargetNotFound(Exception):
    pass


class CommOpenHandler(Handler):
    """
    Used when sending a comm_open request from sidecar to kernel. If there is no Comm registered
    for the target_name, the Kernel will say as much in a stream (stderr) reply, and send a
    comm_close event.
    """

    def __init__(self):
        self.comm_err_msg = None
        self.comm_closed_id = None

    async def handle_stream(self, msg: messages.Stream):
        if msg.content.name == "stderr":
            self.comm_err_msg = msg.content.text

    async def handle_comm_close(self, msg: messages.CommClose):
        self.comm_closed_id = msg.content.comm_id


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
    _handler_timeout: Optional[float] = None  # optional timeout when awaiting Action handlers
    jupyter_widget_handler: Type[CommHandler] = WidgetHandler

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
        self.is_processing = False  # turns to True entering context manager, False when exiting
        self.mq_task: asyncio.Task = None  # picks things up off the PriorityQueue to process
        # One parent task with two child tasks per ZMQ channel:
        # - watch for zmq disconnect
        # - pick up zmq messages off socket and drop onto PriorityQueue
        self.channel_watching_tasks: List[asyncio.Task] = []
        self.channel_watcher_parent_tasks: List[asyncio.Task] = []

        # Handlers to attach to every Action. These will be appended to action.handlers
        # during .send, which means they'll run /after/ other handlers.
        # Fail right away if there's common mistake of init'ing with default_handlers=Handler()
        if default_handlers and not isinstance(default_handlers, list):
            raise RuntimeError(f"{default_handlers=} must be None or a list")
        self.default_handlers = default_handlers or []
        self.comm_manager = comm_manager or CommManager(
            handlers={"jupyter.widget": self.jupyter_widget_handler}
        )

        # self.kc.<channel>.is_alive() returns True even after a disconnect.
        # Keeping our own "connected" state for each channel so Applications using this Client can
        # customize behavior on ZMQ disconnect / reconnect. This gets set once when entering the
        # class async context manager, and then further updated by zmq_lifecycle_hooks.
        self.zmq_channels_connected = {
            "shell": False,
            "iopub": False,
            "stdin": False,
            "control": False,
        }

    @property
    def running_action(self) -> Optional[actions.KernelAction]:
        """
        Return a best guess at what action the Kernel is handling right now.

        The logic here is that actions is a dict, which is ordered in Python 3.6+, so iterate
        through them until we find an action that is not done yet. The first non-done action is
        the one we're looking for.
        """
        for action in self.actions.values():
            if action.running:
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

            log_msg = f"Sent {action.request.header.msg_type} to kernel"
            log_extra = {}
            if get_settings().pprint_logs:
                log_extra["body"] = pprint.pformat(action.request.dict())
            logger.debug(log_msg, extra=log_extra)
        except Exception as e:
            log_msg = f"Error sending {action.request.header.msg_type} message over ZMQ"
            log_extra = {}
            if get_settings().pprint_logs:
                log_extra["body"] = pprint.pformat(action.request.dict())
            logger.error(log_msg, extra=log_extra, exc_info=True)
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

    def shutdown_request(
        self, restart: bool = True, handlers: List[Handler] = None
    ) -> actions.KernelAction:
        req = requests.ShutdownRequest(content={"restart": restart})
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
            self._watch_channel_for_status(
                channel_name,
                channel.socket.get_monitor_socket(),
            )
        )
        self.channel_watching_tasks.append(message_task)
        self.channel_watching_tasks.append(status_task)

        done, pending = await asyncio.wait(
            [message_task, status_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        logger.debug(
            f"Cycling {channel_name} based on task ending", extra={"channel": channel_name}
        )

        # Reconnect ASAP
        # The .<channel_name>_channel properties check if ._<channel_name>_channel attribute
        # is None. If it is None, it starts the connection on that channel. Setting this attr
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

    async def _watch_channel_for_status(self, channel_name: str, monitor_socket: zmq.Socket):
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
                # Set the channel connected status to True or False based on specific events,
                # also return out of this coroutine on disconnect to trigger the reconnect process
                if event == zmq.EVENT_HANDSHAKE_SUCCEEDED:
                    self.zmq_channels_connected[channel_name] = True
                if event == zmq.EVENT_DISCONNECTED:
                    self.zmq_channels_connected[channel_name] = False
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
                self.mq.put_nowait(raw_msg)
                msg_type = raw_msg.get("msg_type", "")

                # When using kernel-sidecar in a production app, we noticed that pprint.pformat
                # caused OOMs due to pprint.pformat trying to format large messages (e.g.
                # display_data for large Dataframes formatted with dx.py).
                log_msg = f"Message {msg_type} on {channel_name}"
                log_extra = {"channel": channel_name}
                if get_settings().pprint_logs:
                    log_extra["body"] = pprint.pformat(raw_msg)
                logger.debug(log_msg, extra=log_extra)
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
            # Pull dictionaries off the internal message queue
            raw_msg: dict = await self.mq.get()

            # kernel status "starting" is one example of messages with no parent header
            if not raw_msg.get("parent_header"):
                await self.handle_missing_parent_msg_id(raw_msg)
                continue

            # Getting a ValidationError here probably means we need to add new Message models
            try:
                msg = pydantic.parse_obj_as(messages.Message, raw_msg)
            except pydantic.ValidationError as e:
                await self.handle_unparseable_message(raw_msg, e)

            # Getting an "untracked action" probably means another client is talking to the Kernel
            # over ZMQ and sending in requests
            if msg.parent_header.msg_id not in self.actions:
                await self.handle_untracked_action(msg)
                continue

            # Happy path: we have an Action for the parent request of messages we see coming in
            action = self.actions[msg.parent_header.msg_id]

            # Log warning if we think we're seeing responses for a new Action and haven't completed
            # the previously running action, e.g. we start getting status / content responses for
            # a new execute_request when we haven't seen execute_reply / status idle for a previous
            # execute request
            if self.running_action and self.running_action is not action:
                logger.warning(
                    f"Observed message for {action} while {self.running_action} has not finished"
                )

            # Optional timeout for callbacks
            try:
                await asyncio.wait_for(action.handle_message(msg), timeout=self._handler_timeout)
            except asyncio.CancelledError:
                logger.warning(f"Timeout handling callbacks for {action}")
                continue
            except:  # noqa: E722
                # Important decision to not raise the exception here so that one failed callback
                # does not stop the entire process inbound message coroutine loop
                logger.exception("Error while handling message")

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
        # Make a noisy warning here because it will potentially break awaiting actions, such as
        # if kernel_info_reply ends up unparseable because LanguageInfo payload is slightly off or
        # something, then await sidecar.kernel_info_request() will never resolve
        logger.warning(
            "Unparseable message", extra={"body": pprint.pformat(raw_msg), "error": error}
        )

    async def handle_zmq_disconnect(self, channel_name: str):
        pass

    async def setup(self):
        """
        Hook for subclasses/applications to use as an entrypoint after entering the async context
        manager. May be useful for things like -
         1. Execute requests that import libraries
         2. Establishing comms to custom comm targets
         3. Setting state in the Kernel from information in the document model
        """
        pass

    async def __aenter__(self) -> "KernelSidecarClient":
        # General asyncio comment: make sure tasks always have a reference (assigned to variable or
        # being awaited) or they might be garbage collected while running.
        # @kafonek: this seems like a situation where 3.11+ asyncio.TaskGroup's might help but tried
        # it briefly and it didn't yield out of the context manager like I expected. Also not sure I
        # want to pin to 3.11+ quite yet.
        for channel in ["iopub", "shell", "control", "stdin"]:
            task = asyncio.create_task(self.watch_channel(channel))
            self.channel_watcher_parent_tasks.append(task)
            self.zmq_channels_connected[channel] = True
        self.mq_task = asyncio.create_task(self.process_message())
        await self.setup()  # in prod, this would be things like importing libs and registering Comms
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
