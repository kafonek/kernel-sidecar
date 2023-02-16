"""
Use:

async with kernel_sidecar.Kernel(connection_info) as kernel:
    action = kernel.kernel_info_request()
    await action

print(action.content)
"""

import asyncio
import pprint
from typing import List, Optional, TypedDict

import pydantic
import structlog
import zmq
from jupyter_client import AsyncKernelClient, KernelConnectionInfo
from jupyter_client.channels import ZMQSocketChannel
from kernel_sidecar import actions, messages
from zmq.utils.monitor import recv_monitor_message

logger = structlog.getLogger(__name__)


# purely for type hinting kernel_client.session.msg return value
class TypedJupyterClientMessage(TypedDict):
    buffers: list
    content: dict
    header: dict
    metadata: dict
    msg_id: str
    msg_type: str
    parent_header: dict


class Kernel:
    """
    Primary interface between our Sidecar and a Kernel.
     - Manages the zmq connections between Sidecar and Kernel
     - Takes in Action models and creates request messages sent to Kernel
     - Watches ZMQ messages for messages coming in from the Kernel
     - Parses messages into Pydantic models
     - Delegates handling those Pydantic models to the appropriate Action that spawned
       the request-reply pattern
    """

    _message_model = messages.Message
    # ^^ discriminator model to parse messages coming in from ZMQ. Override this if you have
    # custom message models defined somewhere besides kernel_sidecar.messages
    # should be type: Annotated[Union[models...], Field(discriminator='msg_type')]
    _handler_timeout: float = None  # optional timeout when awaiting Action handlers

    def __init__(self, connection_info: KernelConnectionInfo):
        self.kc = AsyncKernelClient()
        self.kc.load_connection_info(connection_info)

        self.status: messages.KernelStatus = None
        self.actions: dict[str, actions.KernelActionBase] = {}

        self.kc.start_channels()

        # message queue, raw data (dict) from all zmq channels drop into here
        self.mq = asyncio.Queue()

        # Keep track of tasks to cancel while shutting down
        self.mq_task: asyncio.Task = None
        self.channel_watching_tasks: List[asyncio.Task] = []
        self.channel_watcher_parent_tasks: List[asyncio.Task] = []

    def send_stdin(self, value: str) -> None:
        msg: TypedJupyterClientMessage = self.kc.session.msg("input_reply")
        msg["content"]["value"] = value
        try:
            self.kc.stdin_channel.send(msg)
        except Exception:
            logger.exception("Error sending stdin")

    def request(self, action: actions.KernelActionBase) -> actions.KernelActionBase:
        """
        Build a kernel_client.session.msg request dictionary using parameters from the Action model
        and send that request to the kernel.

        The msg_id from the request dictionary will be set on the Action, and any observed messages
        from ZMQ with a parent_header.msg_id matching that id will be delegated to the Action for
        handling.
        """
        # Build request dictionary using jupyter_client, passing in msg_type from action
        # and content, header, metadata from action.request
        msg: TypedJupyterClientMessage = self.kc.session.msg(action.request_msg_type)

        # msg_id is generated while creating the request dict, set msg_id on the Action model
        msg_id = msg["msg_id"]
        action.msg_id = msg_id

        # update the msg dictionary with any content / header / metadata from the Action model
        msg["content"].update(action.request_content.dict())
        msg["metadata"].update(action.request_metadata.dict())
        msg["header"].update(action.request_header.dict(exclude_unset=True))

        # Add to our internal dict for message routing (parent_header.msg_id -> action handlers)
        self.actions[msg_id] = action

        # Send the request over the appropriate zmq channel
        try:
            channel: ZMQSocketChannel = getattr(self.kc, f"{action.request_channel}_channel")
            channel.send(msg)
            logger.info("Sent request to kernel", msg=pprint.pformat(msg))
        except Exception as e:
            logger.exception("Error sending message", msg=msg)
            raise e
        return action

    def kernel_info_request(self) -> actions.KernelInfoAction:
        return self.request(actions.KernelInfoAction())

    def execute_request(self, code: str) -> actions.ExecuteAction:
        action = actions.ExecuteAction()
        action.request_content.code = code
        return self.request(action)

    def complete_request(
        self, code: str, cursor_pos: Optional[int] = None
    ) -> actions.CompleteAction:
        action = actions.CompleteAction()
        action.request_content.code = code
        action.request_content.cursor_pos = cursor_pos
        return self.request(action)

    def interrupt_request(self) -> actions.InterruptAction:
        return self.request(actions.InterruptAction())

    def comm_open_request(
        self, target_name: str, data: Optional[dict] = None
    ) -> actions.CommOpenAction:
        action = actions.CommOpenAction()
        action.request_content.target_name = target_name
        if data:
            action.request_content.data = data
        return self.request(action)

    def comm_msg_request(self, comm_id: str, data: Optional[dict] = None) -> actions.CommMsgAction:
        action = actions.CommMsgAction()
        action.request_content.comm_id = comm_id
        if data:
            action.request_content.data = data
        return self.request(action)

    async def watch_channel(self, channel_name: str):
        """
        Watch a specific ZMQ channel, picking up messages coming in from the kernel and dropping
        those onto the internal asyncio.Queue for processing (delegating to Action handlers).

        Cycles the ZMQ connection if it's lost.
        """
        structlog.contextvars.bind_contextvars(channel_name=channel_name)
        logger.info("Channel watcher started")
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

        logger.info("Cycling channel based on task ending")
        self.channel_watching_tasks.remove(message_task)
        self.channel_watching_tasks.remove(status_task)

        setattr(self.kc, f"_{channel}", None)

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
                logger.debug("Received message from zmq", raw_msg=pprint.pformat(raw_msg))
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
                if msg.msg_type == "status":
                    self.status = msg.content.execution_state

                if msg.parent_msg_id not in self.actions:
                    await self.handle_untracked_action(msg)
                    continue

                action = self.actions[msg.parent_msg_id]
                await asyncio.wait_for(action.handle_message(msg), timeout=1)

            except pydantic.ValidationError as e:
                await self.handle_unparseable_message(raw_msg, e)
            except asyncio.CancelledError:
                logger.info("Message processor cancelled")
                break
            except Exception as e:
                logger.exception("Error while handling message")
                raise e

    async def handle_missing_parent_msg_id(self, raw_msg: dict):
        """
        Almost all messages should have a parent_header.msg_id which we can use to delegate to
        Action handlers. Any messages without a parent_header msg_id should be documented and
        dealt with here.
        """
        if (
            raw_msg["msg_type"] == "status"
            and raw_msg["content"].get("execution_state") == "starting"
        ):
            logger.info("Observed Kernel starting status message")
            self.status = messages.KernelStatus.starting
        else:
            logger.warning("Got message with missing parent header message id", raw_msg=raw_msg)

    async def handle_unparseable_message(self, raw_msg: dict, error: pydantic.ValidationError):
        logger.warning("Failed to parse message from kernel", raw_msg=raw_msg, error=error)

    async def handle_untracked_action(self, msg: messages.Message):
        """
        In theory if we're the only client talking to a kernel, we shouldn't get into this method.
        """
        logger.warning("Got message with untracked parent header message id", msg=msg)

    async def __aenter__(self) -> "Kernel":
        for channel in ["iopub", "shell", "control", "stdin"]:
            task = asyncio.create_task(self.watch_channel(channel))
            self.channel_watcher_parent_tasks.append(task)
        self.mq_task = asyncio.create_task(self.process_message())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for task in self.channel_watcher_parent_tasks:
            task.cancel()
        for task in self.channel_watching_tasks:
            task.cancel()
        if self.mq_task:
            self.mq_task.cancel()
        self.kc.stop_channels()
        self.kc.stop_channels()
