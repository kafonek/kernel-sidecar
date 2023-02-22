"""
Pydantic models for messages observed coming from the Kernel over ZMQ.

The models here use a discriminator pattern so that they can be parsed as a
generic "Message" and the return value will be a specific message type model
(and typically specific content model) based on the msg_type.

The Sidecar module is responsible for observing messages coming in, parsing them
into Message models defined here, and delegating handling to the appropriate Action
that represents the original request and subsequent responses by parent_header.msg_id.

Use:
import datetime
from dateutil import tzutc

raw_data = {'buffers': [],
            'content': {'execution_state': 'idle'},
            'header': {'date': datetime.datetime(2023, 1, 20, 13, 33, 48, 25739, tzinfo=tzutc()),
                        'msg_id': '054eb8a8-080c87e74cf4cd9f28620307_12527_9',
                        'msg_type': 'status',
                        'session': '054eb8a8-080c87e74cf4cd9f28620307',
                        'username': 'kafonek',
                        'version': '5.3'},
            'metadata': {},
            'msg_id': '054eb8a8-080c87e74cf4cd9f28620307_12527_9',
            'msg_type': 'status',
            'parent_header': {'date': datetime.datetime(2023, 1, 20, 13, 33, 47, 775780, tzinfo=tzutc()),
                            'msg_id': '580af966-34f6ef93a033a226a5205abb_12524_2',
                            'msg_type': 'complete_request',
                            'session': '580af966-34f6ef93a033a226a5205abb',
                            'username': 'kafonek',
                            'version': '5.3'}}

import pydantic
from kernel_sidecar.models import messages

msg = pydantic.parse_obj_as(messages.Message, raw_data)
msg
>>> Status(
    buffers=[],
    content=StatusContent(execution_state="idle"),
    header=Header(
        date=datetime.datetime(2023, 1, 20, 13, 33, 48, 25739, tzinfo=tzutc()),
        msg_id="054eb8a8-080c87e74cf4cd9f28620307_12527_9",
        msg_type="status",
        session="054eb8a8-080c87e74cf4cd9f28620307",
        username="kafonek",
        version="5.3",
    ),
    metadata=BaseModel(),
    msg_id="054eb8a8-080c87e74cf4cd9f28620307_12527_9",
    msg_type="status",
    parent_header=Header(
        date=datetime.datetime(2023, 1, 20, 13, 33, 47, 775780, tzinfo=tzutc()),
        msg_id="580af966-34f6ef93a033a226a5205abb_12524_2",
        msg_type="complete_request",
        session="580af966-34f6ef93a033a226a5205abb",
        username="kafonek",
        version="5.3",
    ),
)
"""
import enum
from datetime import datetime
from typing import Annotated, Any, List, Literal, Optional, Union

from pydantic import BaseModel, Field


# Used for both header and parent_header
class Header(BaseModel):
    date: datetime
    msg_id: str
    msg_type: str
    session: str
    username: str
    version: str


class MessageBase(BaseModel):
    buffers: list = Field(default_factory=list)
    content: BaseModel = Field(default_factory=BaseModel)
    header: Header
    metadata: BaseModel = Field(default_factory=BaseModel)
    msg_id: str
    msg_type: str  # must be overwritten as Literal in submodel
    parent_header: Header


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-status
class KernelStatus(str, enum.Enum):
    busy = "busy"
    idle = "idle"
    starting = "starting"


class StatusContent(BaseModel):
    execution_state: KernelStatus

    class Config:
        use_enum_values = True


class Status(MessageBase):
    msg_type: Literal["status"]
    content: StatusContent


# For normal execution requests, the reply order is:
#
# <frontend sends execute_request (shell)>
# - status (iopub): kernel busy
# - execute_input (iopub): broadcast code that was submitted
# - outputs (iopub): execute_result, stream, etc
# - execute_result (shell): status / execution count
# - status (iopub): kernel idle
#
# (the order of which messages we receive from different channels isn't guaranteed,
#  e.g. we may see status idle on iopub before or after execute_result on shell)
#
# https://jupyter-client.readthedocs.io/en/stable/messaging.html#request-reply


class ExecuteInputContent(BaseModel):
    code: str
    execution_count: int


class ExecuteInput(MessageBase):
    msg_type: Literal["execute_input"]
    content: ExecuteInputContent


# Separate status enum for Cell state vs Kernel state, these values come back as part
# of `<action>_reply` messages rather than in their own `status` message type
class CellStatus(str, enum.Enum):
    ok = "ok"
    error = "error"
    aborted = "aborted"


# "When status is ‘error’, the usual content of a successful reply should be omitted,
# instead the following fields should be present:"
# https://jupyter-client.readthedocs.io/en/stable/messaging.html#request-reply
class ErrorContent(BaseModel):
    ename: str
    evalue: str
    traceback: List[str]


class Error(MessageBase):
    msg_type: Literal["error"]
    content: ErrorContent


# "ok" status content --
#
# "Payloads" in execute_request are deprecated according to docs but
# still used pretty widely
class Page(BaseModel):
    """Page is when you use "??" to show help text for a function/object"""

    source: Literal["page"] = "page"
    data: dict  # mimebundle, must include text/plain
    start: int  # line offset to start from


class SetNextInput(BaseModel):
    """
    When you call get_ipython().set_next_input("foo") to create new cell with
    content foo or add replace=True to replace current cell content
    """

    source: Literal["set_next_input"] = "set_next_input"
    text: str
    replace: bool


Payload = Annotated[Union[Page, SetNextInput], Field(discriminator="source")]


class ExecuteReplyOkContent(BaseModel):
    status: Literal[CellStatus.ok] = CellStatus.ok
    execution_count: int
    payload: List[Payload] = Field(default_factory=list)
    user_expressions: dict = Field(default_factory=dict)

    class Config:
        use_enum_values = True


class ExecuteReplyErrorContent(BaseModel):
    status: Literal[CellStatus.error] = CellStatus.error
    execution_count: int
    payload: List[Payload] = Field(default_factory=list)
    user_expressions: dict = Field(default_factory=dict)
    ename: str
    engine_info: dict
    evalue: str
    traceback: List[str]

    class Config:
        use_enum_values = True


class ExecuteReplyAbortedContent(BaseModel):
    status: Literal[CellStatus.aborted] = CellStatus.aborted

    class Config:
        use_enum_values = True


ExecuteReplyContent = Annotated[
    Union[ExecuteReplyOkContent, ExecuteReplyErrorContent, ExecuteReplyAbortedContent],
    Field(discriminator="status"),
]


class ExecuteReply(MessageBase):
    msg_type: Literal["execute_reply"]
    content: ExecuteReplyContent


# Cell outputs and displays
# - execute_result when the last line of a cell is an expression (1 + 1)
# - stream when there's things like print('hello')
# - display_data and update_display_data when using IPython.display or widgets
class ExecuteResultContent(BaseModel):
    output_type: Literal["execute_result"] = "execute_result"
    execution_count: int
    data: dict  # mimebundle
    metadata: dict = Field(default_factory=dict)


class ExecuteResult(MessageBase):
    msg_type: Literal["execute_result"]
    content: ExecuteResultContent


class StreamChannel(str, enum.Enum):
    stdout = "stdout"
    stderr = "stderr"


class StreamContent(BaseModel):
    output_type: Literal["stream"] = "stream"
    name: StreamChannel
    text: str

    class Config:
        use_enum_values = True


class Stream(MessageBase):
    msg_type: Literal["stream"]
    content: StreamContent


class DisplayDataContent(BaseModel):
    output_type: Literal["display_data"] = "display_data"
    data: dict  # mimebundle
    metadata: dict = Field(default_factory=dict)
    transient: dict = Field(default_factory=dict)


class DisplayData(MessageBase):
    msg_type: Literal["display_data"]
    content: DisplayDataContent


class UpdateDisplayDataContent(DisplayDataContent):
    output_type: Literal["update_display_data"] = "update_display_data"


class UpdateDisplayData(MessageBase):
    msg_type: Literal["update_display_data"]
    content: UpdateDisplayDataContent


# Comms - https://jupyter-client.readthedocs.io/en/stable/messaging.html#custom-messages
class CommOpenContent(BaseModel):
    comm_id: str
    target_name: str
    data: Any


class CommOpen(MessageBase):
    msg_type: Literal["comm_open"]
    content: CommOpenContent


class CommMsgContent(BaseModel):
    comm_id: str
    data: Any


class CommMsg(MessageBase):
    msg_type: Literal["comm_msg"]
    content: CommMsgContent


class CommCloseContent(BaseModel):
    comm_id: str
    data: Any


class CommClose(MessageBase):
    msg_type: Literal["comm_close"]
    content: CommCloseContent


class CommInfoReplyContent(BaseModel):
    comms: dict  # {comm-id: {target_name: str}}


class CommInfoReply(MessageBase):
    msg_type: Literal["comm_info_reply"]
    content: CommInfoReplyContent


# Kernel info - https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-info
class LanguageInfo(BaseModel):
    name: str
    version: str
    mimetype: str
    file_extension: str
    pygments_lexer: str
    codemirror_mode: Union[str, dict]
    nbconvert_exporter: str


class KernelInfoReplyContent(BaseModel):
    banner: str
    help_links: List[dict] = Field(default_factory=list)
    implementation: str
    implementation_version: str
    language_info: LanguageInfo
    protocol_version: str
    status: str
    debugger: Optional[bool] = None


class KernelInfoReply(MessageBase):
    msg_type: Literal["kernel_info_reply"]
    content: KernelInfoReplyContent


# Inspect
class InspectReplyContent(BaseModel):
    status: str
    found: bool
    data: dict  # mimebundle
    metadata: dict


class InspectReply(MessageBase):
    msg_type: Literal["inspect_reply"]
    content: InspectReplyContent


# Autocomplete
class CompleteReplyContent(BaseModel):
    status: str
    matches: List[str]
    cursor_start: int
    cursor_end: int
    metadata: dict


class CompleteReply(MessageBase):
    msg_type: Literal["complete_reply"]
    content: CompleteReplyContent


# History, not really used in many places
class HistoryReplyContent(BaseModel):
    status: str
    history: list[tuple] = Field(default_factory=list)


class HistoryReply(BaseModel):
    msg_type: Literal["history_reply"]
    content: HistoryReplyContent


# Interrupts
class InterruptReply(MessageBase):
    msg_type: Literal["interrupt_reply"]


# Shutdown
class ShutdownContent(BaseModel):
    status: str
    restart: bool


class Shutdown(MessageBase):
    msg_type: Literal["shutdown_reply"]
    content: ShutdownContent


# Debug reply - https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request
class DumpCellBody(BaseModel):
    sourcePath: str


class DumpCell(BaseModel):
    type: Literal["response"]
    command: Literal["dumpCell"]
    success: bool
    body: DumpCellBody


class Breakpoints(BaseModel):
    source: str
    breakpoints: List[str]


class DebugInfoBody(BaseModel):
    isStarted: bool
    hashMethod: str
    hashSeed: str
    tmpFilePrefix: str
    tmpFileSuffix: str
    breakpoints: List[Breakpoints]
    stoppedThreads: List[int]
    richRendering: bool
    exceptionPaths: List[str]


class DebugInfo(BaseModel):
    type: Literal["response"]
    command: Literal["debugInfo"]
    success: bool
    body: DebugInfoBody


DebugReplyContent = Annotated[Union[DumpCell, DebugInfo], Field(alias="command")]


class DebugReply(MessageBase):
    msg_type: Literal["debug_reply"]
    content: DebugReplyContent


# Input (stdin)
class InputRequestContent(BaseModel):
    prompt: str
    password: bool


class InputRequest(MessageBase):
    msg_type: Literal["input_request"]
    content: InputRequestContent


# See module docstring. Use:
# msg = pydantic.parse_obj_as(Message, raw_dict_from_zmq)
# msg will be one of the specific message types in the Union below complete with its own
# custom content or other nested models.
Message = Annotated[
    Union[
        Status,
        ExecuteInput,
        ExecuteResult,
        Stream,
        DisplayData,
        UpdateDisplayData,
        ExecuteReply,
        Error,
        CommOpen,
        CommMsg,
        CommClose,
        CommInfoReply,
        KernelInfoReply,
        InspectReply,
        CompleteReply,
        HistoryReply,
        InterruptReply,
        Shutdown,
        DebugReply,
        InputRequest,
    ],
    Field(discriminator="msg_type"),
]
