"""
Pydantic models for messages observed coming from the Kernel over ZMQ.

The models here use a discriminator pattern so that they can be parsed as a
generic "Message" and the return value will be a specific message type model
(and typically specific content model) based on the msg_type.

The Sidecar module is responsible for observing messages coming in, parsing them
into Message models defined here, and delegating handling to the appropriate Action
that represents the original request and subsequent responses by parent_header.msg_id.

Use:

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

msg = pydantic.parse_obj_as(Message, raw_data)
msg
>>> StatusMessage(
        buffers=[],
        content=StatusContent(execution_state=<KernelStatus.idle: 'idle'>),
        header=Header(
            date=datetime.datetime(2023, 1, 20, 13, 33, 48, 25739, tzinfo=tzutc()),
            msg_id="054eb8a8-080c87e74cf4cd9f28620307_12527_9",
            msg_type="status",
            session="054eb8a8-080c87e74cf4cd9f28620307",
            username="kafonek",
            version="5.3",
        ),
        metadata={},
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
"""  # noqa: E501
import enum
from datetime import datetime
from typing import Annotated, Any, List, Literal, Union

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
    content: dict  # may be overridden in submodels
    header: Header
    metadata: dict = Field(default_factory=dict)
    msg_id: str
    msg_type: str  # must be overwritten as Literal in submodel
    parent_header: Header

    @property
    def parent_msg_id(self):
        return self.parent_header.msg_id


# Status
class KernelStatus(str, enum.Enum):
    busy = "busy"
    idle = "idle"
    starting = "starting"


class StatusContent(BaseModel):
    execution_state: KernelStatus

    class Config:
        use_enum_values = True


class StatusMessage(MessageBase):
    msg_type: Literal["status"]
    content: StatusContent


# Any sort of error
class ErrorContent(BaseModel):
    ename: str
    evalue: str
    traceback: List[str]


class ErrorMessage(MessageBase):
    msg_type: Literal["error"]
    content: ErrorContent


# execute_*
# execute_input
class ExecuteInputContent(BaseModel):
    code: str
    execution_count: int


class ExecuteInpuMessage(MessageBase):
    msg_type: Literal["execute_input"]
    content: ExecuteInputContent


# "Payloads" in execute_request are deprecated according to docs but
# still used pretty widely
class Pager(BaseModel):
    """Pager is when you use "??" to show help text for a function/object"""

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


Payload = Annotated[Union[Pager, SetNextInput], Field(discriminator="source")]


# execute_reply
# - Content can have different schemas based on the status (ok, aborted, error)
class CellStatus(str, enum.Enum):
    ok = "ok"
    error = "error"
    aborted = "aborted"


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


class ExecuteReplyMessage(MessageBase):
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


class ExecuteResultMessage(MessageBase):
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


class StreamMessage(MessageBase):
    msg_type: Literal["stream"]
    content: StreamContent


class DisplayDataContent(BaseModel):
    output_type: Literal["display_data"] = "display_data"
    data: dict  # mimebundle
    metadata: dict = Field(default_factory=dict)
    transient: dict = Field(default_factory=dict)


class DisplayDataMessage(MessageBase):
    msg_type: Literal["display_data"]
    content: DisplayDataContent


class UpdateDisplayDataContent(DisplayDataContent):
    output_type: Literal["update_display_data"] = "update_display_data"


class UpdateDisplayDataMessage(MessageBase):
    msg_type: Literal["update_display_data"]
    content: UpdateDisplayDataContent


# Comms
class CommOpenContent(BaseModel):
    comm_id: str
    target_name: str
    data: Any


class CommOpenMessage(MessageBase):
    msg_type: Literal["comm_open"]
    content: CommOpenContent


class CommMsgContent(BaseModel):
    comm_id: str
    data: Any


class CommMsgMessage(MessageBase):
    msg_type: Literal["comm_msg"]
    content: CommMsgContent


class CommCloseContent(BaseModel):
    comm_id: str
    data: Any


class CommCloseMessage(MessageBase):
    msg_type: Literal["comm_close"]
    content: CommCloseContent


class CommInfoReplyContent(BaseModel):
    comms: dict  # {comm-id: {target_name: str}}


class CommInfoReplyMessage(MessageBase):
    msg_type: Literal["comm_info_reply"]
    content: CommInfoReplyContent


# Kernel info
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
    debugger: bool = False


class KernelInfoReplyMessage(MessageBase):
    msg_type: Literal["kernel_info_reply"]
    content: KernelInfoReplyContent


# Inspect
class InspectReplyContent(BaseModel):
    status: str
    found: bool
    data: dict  # mimebundle
    metadata: dict


class InspectReplyMessage(MessageBase):
    msg_type: Literal["inspect_reply"]
    content: InspectReplyContent


# Autocomplete
class CompleteReplyContent(BaseModel):
    status: str
    matches: List[str]
    cursor_start: int
    cursor_end: int
    metadata: dict


class CompleteReplyMessage(MessageBase):
    msg_type: Literal["complete_reply"]
    content: CompleteReplyContent


# History, not really used in many places
class HistoryReplyContent(BaseModel):
    status: str
    history: list[tuple] = Field(default_factory=list)


class HistoryReplyMessage(BaseModel):
    msg_type: Literal["history_reply"]
    content: HistoryReplyContent


# Interrupts
class InterruptReplyMessage(MessageBase):
    msg_type: Literal["interrupt_reply"]


# Shutdown
class ShutdownContent(BaseModel):
    status: str
    restart: bool


class ShutdownMessage(MessageBase):
    msg_type: Literal["shutdown_reply"]
    content: ShutdownContent


# Input Request
class InputRequestContent(BaseModel):
    prompt: str
    password: bool


class InputRequestMessage(MessageBase):
    msg_type: Literal["input_request"]
    content: InputRequestContent


# See module docstring. Use:
# msg = pydantic.parse_obj_as(Message, raw_dict_from_zmq)
# msg will be one of the specific message types in the Union below.
Message = Annotated[
    Union[
        StatusMessage,
        ErrorMessage,
        ExecuteInpuMessage,
        ExecuteReplyMessage,
        ExecuteResultMessage,
        StreamMessage,
        DisplayDataMessage,
        UpdateDisplayDataMessage,
        CommOpenMessage,
        CommMsgMessage,
        CommInfoReplyMessage,
        CommCloseMessage,
        KernelInfoReplyMessage,
        InspectReplyMessage,
        CompleteReplyMessage,
        HistoryReplyMessage,
        InterruptReplyMessage,
        ShutdownMessage,
        InputRequestMessage,
    ],
    Field(discriminator="msg_type"),
]
