"""
Actions encapsulate the request-reply process between the kernel_sidecar and actual kernel. 

Each Action model has information that the sidecar needs to generate a request 
(e.g. `execute_request`, `kernel_info_request`, etc) to the kernel. When the sidecar builds 
the request, it will generate a `msg_id` that is partly derived from the current state of 
messages already sent to the kernel. Whenever the sidecar receives new messages from the kernel, 
it will check the `parent_header_message.msg_id` and delegate the message to be handled by 
callbacks for the Action with the matching `msg_id`.

Observers can be attached to an Action to also trigger arbitrary callbacks when different 
message types are handled. That is useful for higher level applications, for instance updating 
an in-memory document model or Y-Doc when `stream` messages are coming in from an execute request.

Use:

kernel = kernel_sidecar.Kernel(connection_info)
await kernel.request(KernelInfoAction())
await action
print(action.content)
"""
import asyncio
import uuid
from datetime import datetime
from typing import Callable, Optional, Union

import structlog
from pydantic import BaseModel, Field, PrivateAttr

from kernel_sidecar import messages

logger = structlog.getLogger(__name__)


class Observer(BaseModel):
    """
    Model of a callback that should be run when specific message types are handled by an Action.

    If message_type is None or empty string, the callback will be run for all message types.

    Observer callbacks are awaited in the order that they're registered, after the message handler
    in the action is done running.
    """

    message_type: Optional[str] = None
    fn: Callable  # TODO: type this correctly. async fn accepting one arg (msg) and args/kwargs
    kwargs: dict = Field(default_factory=dict)


class RequestHeader(BaseModel):
    """
    Header dictionary that you would see from kernel_client.session.msg('msg_type')
    """

    msg_id: Optional[str] = None
    msg_type: Optional[str] = None
    username: Optional[str] = None
    session: Optional[str] = None
    date: Optional[datetime] = None
    version: Optional[str] = None


class KernelActionBase(BaseModel):
    """
    Base model to represent a request / reply pattern between the Sidecar and Kernel.
     - implements an awaitable pattern that resolves and returns Self when kernel is idle
       and an expected reply is seen
     - submodels need to define message handlers, including the expected reply handler that
       will set the _reply_seen event and call maybe_set_future
     - observers can be attached to run asyncio callbacks on specific message types or all
       message types
    """

    request_channel: str = "shell"  # may be 'control' for things like interrupt_request
    request_msg_type: str = ""  # define in submodels, e.g. 'execute_request'
    # Optional content/header/metadata that will be merged into kernel_client.session.msg dict
    # when the request is being built.
    # These are BaseModel in case submodels want to model their content / etc, a bare BaseModel
    # is effectively an empty dict.
    request_content: BaseModel = Field(default_factory=BaseModel)
    request_header: RequestHeader = Field(default_factory=RequestHeader)
    request_metadata: BaseModel = Field(default_factory=BaseModel)
    # Note on the request_* syntax above: I first put those into their own RequestParam model and
    # tried calling kernel_client.session.msg(**action.request.dict()) but that overrode the Header
    # and didn't give back a msg_id, so it's structured this way instead. See kernel.request method
    # for how msg is built and then updated with the model/dicts above.

    msg_id: Optional[str] = None  # set by Kernel after it calls into kernel_client.session.msg

    # Comm open, comm close, and comm msg can theoertically come in from the kernel during any msg
    comms: list[
        Union[messages.CommOpenContent, messages.CommCloseContent, messages.CommMsgContent]
    ] = Field(default_factory=list)

    _kernel_idle: asyncio.Event = PrivateAttr(default_factory=asyncio.Event)
    _reply_seen: asyncio.Event = PrivateAttr(default_factory=asyncio.Event)
    _future: asyncio.Future = PrivateAttr(default_factory=asyncio.Future)
    _observers: list[Observer] = PrivateAttr(default_factory=list)

    def __await__(self) -> "KernelActionBase":
        """Support 'await action' syntax"""
        return self._future.__await__()

    def __hash__(self) -> int:
        """Primarily here to support asyncio.gather(action1, action2)"""
        return hash(self.json(sort_keys=True))

    def maybe_set_future(self):
        """
        Resolve this Action as completed. Allows for syntax like:

        action: ExecuteAction = sidecar.execute(source="1 + 1")
        await action

        An Action is usually considered complete when the Kernel is observed reporting idle
        and some other reply message has come in, such as execute_reply or kernel_info_reply.
        There's no guarentee that we'll see the kernel status or the reply message first though.

        Subclasses should define handling the appropriate reply message and calling this method.
        """
        if self._kernel_idle.is_set() and self._reply_seen.is_set():
            self._future.set_result(self)

    def observe(self, fn: Callable, message_type: Optional[str] = None, **kwargs):
        """
        Attach an async callback to be run when a specific message type is handled by this Action,
        or on any message if message_type is None.

        The callback will receive one argument (the message) and have kwargs passed through.
        """
        obs = Observer(fn=fn, message_type=message_type, kwargs=kwargs)
        self._observers.append(obs)
        return obs

    def remove_observer(self, obs: Observer):
        self._observers.remove(obs)

    async def handle_message(self, msg: messages.Message):
        """Delegate message to the appropriate handler defined in subclasses"""
        structlog.contextvars.bind_contextvars(msg=msg.dict(), action=self.dict())
        handler = getattr(self, f"handle_{msg.msg_type}", None)
        if handler:
            try:
                await handler(msg)
            except Exception:
                logger.exception("Error handling message")
                return
        else:
            await self.unhandled_message(msg)
        for obs in self._observers:
            # await obs callbacks if it has no message_type defined or if the message type matches
            if not obs.message_type or msg.msg_type == obs.message_type:
                await obs.fn(msg, **obs.kwargs)

    async def unhandled_message(self, msg: messages.Message):
        """
        Called when a message is delegated to this Action but no handler is defined for the msg_type
        """
        logger.warning("Unhandled message", msg_type=msg.msg_type)

    async def handle_status(self, msg: messages.StatusMessage):
        """
        Most (all?) Actions will do the same thing with kernel status messages: resolve the action
        as complete when kernel is idle and an expected reply has been seen.
        """
        if msg.content.execution_state == messages.KernelStatus.idle:
            self._kernel_idle.set()
            self.maybe_set_future()

    async def handle_shutdown_reply(self, msg: messages.ShutdownMessage):
        logger.warning("Kernel shutdown in progress")

    async def handle_comm_open(self, msg: messages.CommOpenMessage):
        self.comms.append(msg.content)

    async def handle_comm_close(self, msg: messages.CommCloseMessage):
        self.comms.append(msg.content)

    async def handle_comm_msg(self, msg: messages.CommMsgMessage):
        self.comms.append(msg.content)


class KernelInfoAction(KernelActionBase):
    request_channel = "shell"
    request_msg_type = "kernel_info_request"
    # set when the reply is received
    content: messages.KernelInfoReplyContent = None

    async def handle_kernel_info_reply(self, msg: messages.KernelInfoReplyMessage):
        self.content = msg.content
        self._reply_seen.set()
        self.maybe_set_future()


class ExecuteRequestContent(BaseModel):
    code: str = ""
    silent: bool = False
    store_history: bool = True
    user_expressions: dict = Field(default_factory=dict)
    allow_stdin: bool = True
    stop_on_error: bool = True


OutputTypes = Union[
    messages.StreamContent,
    messages.DisplayDataContent,
    messages.ExecuteResultContent,
    messages.ErrorContent,
]


class ExecuteAction(KernelActionBase):
    request_channel = "shell"
    request_msg_type = "execute_request"
    request_content: ExecuteRequestContent = Field(default_factory=ExecuteRequestContent)
    # set from handlers while replies are still coming in
    # streaming output is a list of either StreamContent or DisplayDataContent
    execution_count: int = None
    status: messages.CellStatus = None
    error: messages.ErrorContent = None
    outputs: list[OutputTypes] = Field(default_factory=list)

    async def handle_execute_input(self, msg: messages.ExecuteInpuMessage):
        self.execution_count = msg.content.execution_count

    async def handle_error(self, msg: messages.ErrorMessage):
        self.outputs.append(msg.content)
        self.error = msg.content

    async def handle_execute_result(self, msg: messages.ExecuteResultMessage):
        self.outputs.append(msg.content)

    async def handle_stream(self, msg: messages.StreamMessage):
        self.outputs.append(msg.content)

    async def handle_display_data(self, msg: messages.DisplayDataMessage):
        self.outputs.append(msg.content)

    async def handle_update_display_data(self, msg: messages.UpdateDisplayDataMessage):
        self.outputs.append(msg.content)

    async def handle_execute_reply(self, msg: messages.ExecuteReplyMessage):
        self.status = msg.content.status
        self._reply_seen.set()
        self.maybe_set_future()


class CompleteRequestContent(BaseModel):
    code: str = ""
    cursor_pos: Optional[int] = None


class CompleteAction(KernelActionBase):
    request_channel = "shell"
    request_msg_type = "complete_request"
    request_content: CompleteRequestContent = Field(default_factory=CompleteRequestContent)
    # set when the reply is received
    content: messages.CompleteReplyContent = None

    async def handle_complete_reply(self, msg: messages.CompleteReplyMessage):
        self.content = msg.content
        self._reply_seen.set()
        self.maybe_set_future()


class InterruptAction(KernelActionBase):
    request_channel = "control"
    request_msg_type = "interrupt_request"

    async def handle_interrupt_reply(self, msg: messages.InterruptReplyMessage):
        self._reply_seen.set()
        self.maybe_set_future()


# Comms are a little different from our other actions because there is not necessarily a
# "reply" message. For instance in a comm_open, there can simply be a kernel busy/idle with
# no other message. There can also be zero, one, or more messages emitted by the kernel
# during comm open or comm msg handling. If there's an error, it comes through as a stream
# message.
class CommActionBase(KernelActionBase):
    error: str = None

    async def handle_status(self, msg: messages.StatusMessage):
        if msg.content.execution_state == messages.KernelStatus.idle:
            self._future.set_result(self)

    async def handle_stream(self, msg: messages.StreamMessage):
        if msg.content.name == "stderr":
            self.error = msg.content.text


class CommOpenRequestContent(BaseModel):
    comm_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    target_name: str = ""
    data: dict = Field(default_factory=dict)


class CommOpenAction(CommActionBase):
    request_channel = "shell"
    request_msg_type = "comm_open"
    request_content: CommOpenRequestContent = Field(default_factory=CommOpenRequestContent)

    @property
    def comm_id(self):
        return self.request_content.comm_id


class CommMsgRequestContent(BaseModel):
    comm_id: str = ""
    data: dict = Field(default_factory=dict)


class CommMsgAction(CommActionBase):
    request_channel = "shell"
    request_msg_type = "comm_msg"
    request_content: CommMsgRequestContent = Field(default_factory=CommMsgRequestContent)

    @property
    def comm_id(self):
        return self.request_content.comm_id
