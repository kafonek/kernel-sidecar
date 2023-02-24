"""
Pydantic models for requests that will be sent to the Kernel.

Most requests can be made using convenience functions on the KernelSidecarClient,
e.g. action = kernel.kernel_info_request(); await action

Example use creating a Request and Action manually, then sending with kernel.send:

async with kernel_sidecar.KernelSidecarClient(connection_info) as kernel:
    async def cb(msg: messages.Message):
        print(msg)

    req = ExecuteRequest(content={'code': '2 + 2'})
    action = Action(request=req, handlers=[cb])
    kernel.send(action) # sends over zmq here

    await action 
    # will print out all the received messags for this request-reply sequence,
    # should be something like
    # - status (busy)
    # - exeecute_input
    # - execute_result
    # - execute_reply
    # - status (idle)
"""

import uuid
from datetime import datetime
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, PrivateAttr

SESSION_ID = str(uuid.uuid4())


class RequestHeader(BaseModel):
    """
    Header dictionary that you would see from kernel_client.session.msg('msg_type')
    """

    msg_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    msg_type: str = ""  # override in sub-models
    username: str = "kernel-sidecar"
    session: str = SESSION_ID
    date: datetime = Field(default_factory=datetime.utcnow)
    version: str = "5.3"


class Metadata(BaseModel):
    # support arbitrary dict here: req = Request(metadata={'anykey': 'no validation error'})
    class Config:
        allow_extra = True


class Request(BaseModel):
    content: BaseModel = Field(default_factory=BaseModel)  # usually overriden in submodel
    header: RequestHeader = Field(default_factory=RequestHeader)
    metadata: Metadata = Field(default_factory=Metadata)
    parent_header: BaseModel = Field(default_factory=BaseModel)
    _channel: str = PrivateAttr(default="shell")


#
# Request messages that should be sent over shell channel -
#
# https://jupyter-client.readthedocs.io/en/stable/messaging.html#execute
class ExecuteRequestContent(BaseModel):
    code: str = ""
    silent: bool = False
    store_history: bool = True
    user_expressions: dict = Field(default_factory=dict)
    allow_stdin: bool = True
    stop_on_error: bool = True


class ExecuteRequestHeader(RequestHeader):
    msg_type: str = "execute_request"


class ExecuteRequest(Request):
    content: ExecuteRequestContent = Field(default_factory=ExecuteRequestContent)
    header: ExecuteRequestHeader = Field(default_factory=ExecuteRequestHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#introspection
class InspectRequestContent(BaseModel):
    code: str = ""
    cursor_pos: int = 0
    detail_level: int = 0


class InspectRequestHeader(RequestHeader):
    msg_type: str = "inspect_request"


class InspectRequest(Request):
    content: InspectRequestContent = Field(default_factory=InspectRequestContent)
    header: InspectRequestHeader = Field(default_factory=InspectRequestHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#completion
class CompleteRequestContent(BaseModel):
    code: str = ""
    cursor_pos: int = 0


class CompleteRequestHeader(RequestHeader):
    msg_type: str = "complete_request"


class CompleteRequest(Request):
    content: CompleteRequestContent = Field(default_factory=CompleteRequestContent)
    header: CompleteRequestHeader = Field(default_factory=CompleteRequestHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#history
class HistoryRequestContent(BaseModel):
    output: bool = False
    raw: bool = False
    hist_access_type: str = "range"
    session: int = 0
    start: int = 0
    stop: int = 0
    n: int = 0
    pattern: str = ""
    unique: bool = False


class HistoryRequestHeader(RequestHeader):
    msg_type: str = "history_request"


class HistoryRequest(Request):
    content: HistoryRequestContent = Field(default_factory=HistoryRequestContent)
    header: HistoryRequestHeader = Field(default_factory=HistoryRequestHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#code-completeness
class IsCompleteRequestContent(BaseModel):
    code: str = ""


class IsCompleteRequestHeader(RequestHeader):
    msg_type: str = "is_complete_request"


class IsCompleteRequest(Request):
    content: IsCompleteRequestContent = Field(default_factory=IsCompleteRequestContent)
    header: IsCompleteRequestHeader = Field(default_factory=IsCompleteRequestHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-info
class KernelInfoRequestHeader(RequestHeader):
    msg_type: str = "kernel_info_request"


class KernelInfoRequest(Request):
    header: KernelInfoRequestHeader = Field(default_factory=KernelInfoRequestHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-info
class CommInfoRequestContent(BaseModel):
    target_name: str = None


class CommInfoRequestHeader(RequestHeader):
    msg_type: str = "comm_info_request"


class CommInfoRequest(Request):
    content: CommInfoRequestContent = Field(default_factory=CommInfoRequestContent)
    header: CommInfoRequestHeader = Field(default_factory=CommInfoRequestHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#opening-a-comm
class CommOpenContent(BaseModel):
    target_name: str = ""
    data: dict = Field(default_factory=dict)
    comm_id: str = Field(default_factory=lambda: str(uuid.uuid4()))


class CommOpenHeader(RequestHeader):
    msg_type: str = "comm_open"


class CommOpen(Request):
    content: CommOpenContent = Field(default_factory=CommOpenContent)
    header: CommOpenHeader = Field(default_factory=CommOpenHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#comm-messages
class CommMsgContent(BaseModel):
    data: dict = Field(default_factory=dict)
    comm_id: str = ""


class CommMsgHeader(RequestHeader):
    msg_type: str = "comm_msg"


class CommMsg(Request):
    content: CommMsgContent = Field(default_factory=CommMsgContent)
    header: CommMsgHeader = Field(default_factory=CommMsgHeader)


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#tearing-down-comms
class CommCloseContent(BaseModel):
    comm_id: str = ""
    data: dict = Field(default_factory=dict)


class CommCloseHeader(RequestHeader):
    msg_type: str = "comm_close"


class CommClose(Request):
    content: CommCloseContent = Field(default_factory=CommCloseContent)
    header: CommCloseHeader = Field(default_factory=CommCloseHeader)


#
# Request messages that should be sent over control channel -
#
# https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-interrupt
class InterruptRequestHeader(RequestHeader):
    msg_type: str = "interrupt_request"


class InterruptRequest(Request):
    header: InterruptRequestHeader = Field(default_factory=InterruptRequestHeader)
    _channel: str = PrivateAttr(default="control")


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#kernel-shutdown
class ShutdownRequestContent(BaseModel):
    restart: bool = False


class ShutdownRequestHeader(RequestHeader):
    msg_type: str = "shutdown_request"


class ShutdownRequest(Request):
    content: ShutdownRequestContent = Field(default_factory=ShutdownRequestContent)
    header: ShutdownRequestHeader = Field(default_factory=ShutdownRequestHeader)
    _channel: str = PrivateAttr(default="control")


# https://jupyter-client.readthedocs.io/en/stable/messaging.html#debug-request
class DumpCellArguments(BaseModel):
    code: str


class DumpCellContent(BaseModel):
    type: Literal["request"] = "request"
    command: Literal["dumpCell"] = "dumpCell"
    arguments: DumpCellArguments


class DebugInfoContent(BaseModel):
    type: Literal["request"] = "request"
    command: Literal["debugInfo"] = "debugInfo"


class InspectVariablesContent(BaseModel):
    type: Literal["request"] = "request"
    command: Literal["inspectVariables"] = "inspectVariables"


class RichInspectVariablesArguments(BaseModel):
    variableName: str
    frameId: int


class RichInspectVariablesContent(BaseModel):
    type: Literal["request"] = "request"
    command: Literal["richInspectVariables"] = "richInspectVariables"
    arguments: RichInspectVariablesArguments


DebugRequestContent = Annotated[
    Union[DumpCellContent, DebugInfoContent, InspectVariablesContent, RichInspectVariablesContent],
    Field(discriminator="command"),
]


class DebugRequestHeader(RequestHeader):
    msg_type: str = "debug_request"


class DebugRequest(Request):
    content: DebugRequestContent
    header: DebugRequestHeader = Field(default_factory=DebugRequestHeader)
    _channel: str = PrivateAttr(default="control")


# Seems weird to have a "reply" in the requests file but this is indeed coming from
# client (sidecar) to kernel, as a response to the kernel sending out an input_request
# if a user exeecutes code with input() and allow_stdin is set to true for the execute request
class InputReplyContent(BaseModel):
    value: str = ""


class InputReplyHeader(RequestHeader):
    msg_type: str = "input_reply"


class InputReply(Request):
    content: InputReplyContent = Field(default_factory=InputReplyContent)
    header: InputReplyHeader = Field(default_factory=InputReplyHeader)
    _channel: str = PrivateAttr(default="stdin")
