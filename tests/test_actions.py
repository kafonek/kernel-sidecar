import asyncio
import textwrap
from unittest.mock import AsyncMock

import pytest
from kernel_sidecar import actions
from kernel_sidecar.handlers import DebugHandler
from kernel_sidecar.kernel import SidecarKernelClient
from kernel_sidecar.models import messages, requests


async def test_handlers(kernel: SidecarKernelClient):
    """
    Show that we can attach multiple handlers to a single action.
     - As Handler class/subclass instances
     - As generic async callables
     - As init args and/or post-instance handlers list appends
    """
    req = requests.KernelInfoRequest()
    handler1 = DebugHandler()
    handler2 = AsyncMock()
    handler3 = AsyncMock()
    action = actions.KernelAction(req, handlers=[handler1, handler2])
    action.handlers.append(handler3)
    kernel.send(action)
    await action
    assert handler1.counts == {"status": 2, "kernel_info_reply": 1}
    assert handler2.call_count == 3
    assert handler3.call_count == 3


async def test_kernel_info(kernel: SidecarKernelClient):
    """
    Show that the kernel.kernel_info_request() helper method builds an appropriate KernelInfoRequest
    and we see the expected results in an attached handler.
    """
    handler = DebugHandler()
    action = kernel.kernel_info_request(handlers=[handler])
    await action
    assert handler.counts == {"status": 2, "kernel_info_reply": 1}
    kernel_info_reply: messages.KernelInfoReply = handler.last_msg_by_type["kernel_info_reply"]
    assert isinstance(kernel_info_reply, messages.KernelInfoReply)
    assert kernel_info_reply.content.status == "ok"


async def test_execute_statement(kernel: SidecarKernelClient):
    """
    Code that returns a statement as the last line should have that output show up in the content
    of the execute_result message.
    """
    handler = DebugHandler()
    action = kernel.execute_request(code="1+1", handlers=[handler])
    await action
    assert handler.counts == {
        "status": 2,
        "execute_input": 1,
        "execute_result": 1,
        "execute_reply": 1,
    }
    execute_result: messages.ExecuteResult = handler.last_msg_by_type["execute_result"]
    assert isinstance(execute_result, messages.ExecuteResult)
    assert execute_result.content.data == {"text/plain": "2"}


async def test_execute_stream(kernel: SidecarKernelClient):
    """
    Code that prints to stdout should show up in the content of the stream message.
    """
    handler = DebugHandler()
    action = kernel.execute_request(code="print('hello world')", handlers=[handler])
    await action
    assert handler.counts == {
        "status": 2,
        "execute_input": 1,
        "stream": 1,
        "execute_reply": 1,
    }
    stream: messages.Stream = handler.last_msg_by_type["stream"]
    assert isinstance(stream, messages.Stream)
    assert stream.content.name == "stdout"
    assert stream.content.text == "hello world\n"


async def test_execute_display_data(kernel: SidecarKernelClient):
    """
    Code that uses the display() function should show up in the content of the display_data message.
    """
    handler = DebugHandler()
    code = textwrap.dedent(
        """
    from IPython.display import display
    display('hello world')
    """
    )
    action = kernel.execute_request(code=code, handlers=[handler])
    await action
    assert handler.counts == {
        "status": 2,
        "execute_input": 1,
        "display_data": 1,
        "execute_reply": 1,
    }
    display_data: messages.DisplayData = handler.last_msg_by_type["display_data"]
    assert isinstance(display_data, messages.DisplayData)
    assert display_data.content.data == {"text/plain": "'hello world'"}


async def test_execute_display_update(kernel: SidecarKernelClient):
    """
    Displaying an object with a display_id should allow us to update that display with new content,
    which will come in through an update_display_data message.
    """
    handler = DebugHandler()
    code = textwrap.dedent(
        """
    from IPython.display import display
    disp = display('hello world', display_id='test_display')
    disp.update('updated display')
    """
    )
    action = kernel.execute_request(code=code, handlers=[handler])
    await action
    assert handler.counts == {
        "status": 2,
        "execute_input": 1,
        "display_data": 1,
        "update_display_data": 1,
        "execute_reply": 1,
    }
    display_data: messages.DisplayData = handler.last_msg_by_type["display_data"]
    assert display_data.content.data == {"text/plain": "'hello world'"}
    assert display_data.content.transient == {"display_id": "test_display"}
    update_display_data = handler.last_msg_by_type["update_display_data"]
    assert isinstance(update_display_data, messages.UpdateDisplayData)
    assert update_display_data.content.data == {"text/plain": "'updated display'"}
    assert update_display_data.content.transient == {"display_id": "test_display"}


async def test_execute_error(kernel: SidecarKernelClient):
    """
    Code that raises an exception should show up as an error message type with the traceback
    in the content.
    """
    handler = DebugHandler()
    action = kernel.execute_request(code="1 / 0", handlers=[handler])
    await action
    assert handler.counts == {
        "status": 2,
        "execute_input": 1,
        "error": 1,
        "execute_reply": 1,
    }
    error: messages.Error = handler.last_msg_by_type["error"]
    assert isinstance(error, messages.Error)
    assert error.content.ename == "ZeroDivisionError"
    assert len(error.content.traceback) > 0


async def test_input(kernel: SidecarKernelClient):
    """
    Show that the kernel.execute_request() helper method builds an appropriate ExecuteRequest
    and we see the expected results in an attached handler.
    """
    handler = DebugHandler()

    async def reply_to_input_request(msg: messages.Message):
        if msg.msg_type == "input_request":
            kernel.send_stdin("test input")

    code = textwrap.dedent(
        """
    x = input("Enter a value: ")
    x
    """
    )
    action = kernel.execute_request(code, handlers=[handler, reply_to_input_request])
    await action
    assert handler.counts == {
        "status": 2,
        "execute_input": 1,
        "input_request": 1,
        "execute_result": 1,
        "execute_reply": 1,
    }


async def test_complete_request(kernel: SidecarKernelClient):
    """
    Show that the kernel.complete_request() helper method builds an appropriate CompleteRequest
    and we see the expected results in an attached handler.
    """
    handler = DebugHandler()
    code = textwrap.dedent(
        """
    class Foo:
        def bar(self):
            pass

    f = Foo()
    f.
    """.strip()
    )
    action = kernel.complete_request(code, handlers=[handler])
    await action
    assert handler.counts == {"status": 2, "complete_reply": 1}

    complete_reply: messages.CompleteReply = handler.last_msg_by_type["complete_reply"]
    assert "bar" in complete_reply.content.matches


async def test_interrupt(kernel: SidecarKernelClient):
    """
    Show that the kernel.interrupt() helper method sends an interrupt message. Running execute
    requests should come back as status "error" with an error message of "KeyboardInterrupt".
    Any queued execute requests should come back as status "aborted".
    """
    handler1 = DebugHandler()  # first execution, should end up as error / interrupted
    handler2 = DebugHandler()  # second execution, should end up as aborted
    handler3 = DebugHandler()  # interrupt request
    action1 = kernel.execute_request(code="import time; time.sleep(60)", handlers=[handler1])
    action2 = kernel.execute_request(code="1 + 1", handlers=[handler2])
    action3 = kernel.interrupt_request(handlers=[handler3])
    await asyncio.gather(action1, action2, action3)
    assert handler1.counts == {"status": 2, "execute_input": 1, "error": 1, "execute_reply": 1}
    assert handler1.last_msg_by_type["error"].content.ename == "KeyboardInterrupt"
    assert handler1.last_msg_by_type["execute_reply"].content.status == "error"

    assert handler2.counts == {"status": 2, "execute_reply": 1}
    assert handler2.last_msg_by_type["execute_reply"].content.status == "aborted"

    assert handler3.counts == {"status": 2, "interrupt_reply": 1}


async def test_comm(kernel: SidecarKernelClient):
    """
    Show that we can create a Comm on the Kernel side, then open a Comm from the sidecar and
    send a Comm msg from sidecar to Kernel with expected response from Kernel.
    """
    code = textwrap.dedent(
        """
    def comm_fn(comm, open_msg):
        @comm.on_msg
        def _recv(msg):
            comm.send({"echo": msg["content"]["data"]})
        
        comm.send("connected")

    get_ipython().kernel.comm_manager.register_target("test_comm", comm_fn)
    """
    )
    setup_action = kernel.execute_request(code)
    await setup_action

    handler1 = DebugHandler()  # for comm open
    handler2 = DebugHandler()  # for comm msg
    handler3 = DebugHandler()  # for comm close

    action1 = kernel.comm_open_request(target_name="test_comm", handlers=[handler1])
    await action1
    assert handler1.counts == {"status": 2, "comm_msg": 1}
    resp1: messages.CommMsg = handler1.last_msg_by_type["comm_msg"]
    comm_id = resp1.content.comm_id

    action2 = kernel.comm_msg_request(comm_id=comm_id, data={"test": "data"}, handlers=[handler2])
    await action2
    assert handler2.counts == {"status": 2, "comm_msg": 1}
    resp2: messages.CommMsg = handler2.last_msg_by_type["comm_msg"]
    assert resp2.content.data == {"echo": {"test": "data"}}

    action3 = kernel.comm_close_request(comm_id=comm_id, handlers=[handler3])
    await action3
    assert handler3.counts == {"status": 2}


async def test_ipywidgets(kernel: SidecarKernelClient):
    """
    Show how to capture comm messages emitted by ipywidgets
    """
    handler = DebugHandler()
    code = textwrap.dedent(
        """
    from ipywidgets import IntSlider
    slider = IntSlider()
    slider.value = 5
    slider
    """
    )
    action = kernel.execute_request(code, handlers=[handler])
    await action
    assert handler.counts == {
        "status": 2,
        "execute_input": 1,
        # three comm opens happen when initializing any widget, one for layout, one for style,
        # and one for control of the model itself (e.g. updating value, parameters)
        "comm_open": 3,
        "comm_msg": 1,  # comm_msg emitted when calling slider.value
        "execute_result": 1,  # From repring the widget as last line of input code
        "execute_reply": 1,
    }
    execute_result: messages.ExecuteResult = handler.last_msg_by_type["execute_result"]
    assert execute_result.content.data["text/plain"] == "IntSlider(value=5)"
    model_id = execute_result.content.data["application/vnd.jupyter.widget-view+json"]["model_id"]
    # last comm_open is the one for the model itself
    comm_open: messages.CommOpen = handler.last_msg_by_type["comm_open"]
    assert isinstance(comm_open, messages.CommOpen)
    assert comm_open.content.comm_id == model_id
    assert comm_open.content.data["state"]["_model_name"] == "IntSliderModel"
    comm_msg: messages.CommMsg = handler.last_msg_by_type["comm_msg"]
    assert isinstance(comm_msg, messages.CommMsg)
    assert comm_msg.content.comm_id == model_id
    assert comm_msg.content.data == {
        "method": "update",
        "state": {"value": 5},
        "buffer_paths": [],
    }


def test_unrecognized_request_type():
    """
    Show that actions.KernelAction raises a ValueError if the request type is not recognized.
    The reason to raise an error there is to avoid maybe-hard-to-debug situations where an action
    ends up being "unawaitable" because the KernelAction doesn't know when the request-reply cycle
    is complete.
    """
    req = requests.Request(header={"msg_type": "unrecognized_type"})
    with pytest.raises(ValueError) as exc_info:
        actions.KernelAction(req)
    exc_info.match("Unrecognized request type unrecognized_type")


def test_register_new_request_type():
    """
    Show how a custom action can inject its own REPLY_MSG_TYPES to not raise ValueError on init.
    """
    req = requests.Request(header={"msg_type": "unrecognized_type"})
    CUSTOM_REPLY_MSG_TYPES = {"unrecognized_type": "unrecognized_reply"}

    class CustomAction(actions.KernelAction):
        REPLY_MSG_TYPES = actions.REPLY_MSG_TYPES | CUSTOM_REPLY_MSG_TYPES

    action = CustomAction(req)
    assert action.request.header.msg_type == "unrecognized_type"

    # Prove that we didn't mutate the module-level REPLY_MSG_TYPES nor the
    # vanilla actions.KernelAction class attribute
    with pytest.raises(ValueError):
        actions.KernelAction(req)
