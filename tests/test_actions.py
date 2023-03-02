import asyncio
import textwrap
from unittest.mock import AsyncMock

import pytest
from kernel_sidecar import actions
from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.handlers import DebugHandler
from kernel_sidecar.models import messages, requests


async def test_handlers(ipykernel: dict):
    """
    - Show that handlers attached to an Action can be Handler subclass or async callables
    - Show that they can be attached in different ways and the order matters,
      - on Action init
      - appending to Action.handlers after init
      - appending to Action.handlers during kernel.send if there are default_handlers
    """
    handler1 = DebugHandler()
    handler2 = AsyncMock()
    handler3 = DebugHandler()
    handler4 = DebugHandler()
    async with KernelSidecarClient(
        connection_info=ipykernel, default_handlers=[handler4]
    ) as kernel:
        req = requests.KernelInfoRequest()
        action = actions.KernelAction(req, handlers=[handler1, handler2])
        action.handlers.append(handler3)
        kernel.send(action)
        # order matters, the "default_handler" (handler4) should be last
        assert action.handlers == [handler1, handler2, handler3, handler4, kernel.comm_manager]
        await action
    assert handler1.counts == {"status": 2, "kernel_info_reply": 1}
    # For the async fn, show it was called 3 times and the arg in the first message was Status
    assert handler2.call_count == 3
    assert isinstance(handler2.call_args_list[0].args[0], messages.Status)
    assert handler3.counts == {"status": 2, "kernel_info_reply": 1}
    assert handler4.counts == {"status": 2, "kernel_info_reply": 1}


async def test_kernel_info(kernel: KernelSidecarClient):
    """
    Show that the kernel.kernel_info_request() helper method builds an appropriate KernelInfoRequest
    and we see the expected results in an attached handler.
    """
    handler = DebugHandler()
    action = kernel.kernel_info_request(handlers=[handler])
    await action
    assert handler.counts == {"status": 2, "kernel_info_reply": 1}
    kernel_info_reply: messages.KernelInfoReply = handler.get_last_msg("kernel_info_reply")
    assert isinstance(kernel_info_reply, messages.KernelInfoReply)
    assert kernel_info_reply.content.status == "ok"


async def test_execute_statement(kernel: KernelSidecarClient):
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
    execute_result: messages.ExecuteResult = handler.get_last_msg("execute_result")
    assert isinstance(execute_result, messages.ExecuteResult)
    assert execute_result.content.data == {"text/plain": "2"}


async def test_execute_stream(kernel: KernelSidecarClient):
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
    stream: messages.Stream = handler.get_last_msg("stream")
    assert isinstance(stream, messages.Stream)
    assert stream.content.name == "stdout"
    assert stream.content.text == "hello world\n"


async def test_execute_display_data(kernel: KernelSidecarClient):
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
    display_data: messages.DisplayData = handler.get_last_msg("display_data")
    assert isinstance(display_data, messages.DisplayData)
    assert display_data.content.data == {"text/plain": "'hello world'"}


async def test_execute_display_update(kernel: KernelSidecarClient):
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
    display_data: messages.DisplayData = handler.get_last_msg("display_data")
    assert display_data.content.data == {"text/plain": "'hello world'"}
    assert display_data.content.transient == {"display_id": "test_display"}
    update_display_data = handler.get_last_msg("update_display_data")
    assert isinstance(update_display_data, messages.UpdateDisplayData)
    assert update_display_data.content.data == {"text/plain": "'updated display'"}
    assert update_display_data.content.transient == {"display_id": "test_display"}


async def test_execute_error(kernel: KernelSidecarClient):
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
    error: messages.Error = handler.get_last_msg("error")
    assert isinstance(error, messages.Error)
    assert error.content.ename == "ZeroDivisionError"
    assert len(error.content.traceback) > 0


async def test_input(kernel: KernelSidecarClient):
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


async def test_complete_request(kernel: KernelSidecarClient):
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

    complete_reply: messages.CompleteReply = handler.get_last_msg("complete_reply")
    assert "bar" in complete_reply.content.matches


async def test_interrupt(kernel: KernelSidecarClient):
    """
    Show that the kernel.interrupt() helper method sends an interrupt message. Running execute
    requests should come back as status "error" with an error message of "KeyboardInterrupt".
    Any queued execute requests should come back as status "aborted".
    """
    handler1 = DebugHandler()  # first execution, should end up as error / interrupted
    handler2 = DebugHandler()  # second execution, should end up as aborted
    handler3 = DebugHandler()  # interrupt request
    action1 = kernel.execute_request(code="import time; time.sleep(600)", handlers=[handler1])
    action2 = kernel.execute_request(code="1 + 1", handlers=[handler2])
    action3 = kernel.interrupt_request(handlers=[handler3])
    # Fail in a reasonable time if something goes wrong, don't let test run for 10 minutes
    await asyncio.wait_for(asyncio.gather(action1, action2, action3), timeout=10)
    assert handler1.counts == {"status": 2, "execute_input": 1, "error": 1, "execute_reply": 1}
    assert handler1.get_last_msg("error").content.ename == "KeyboardInterrupt"
    assert handler1.get_last_msg("execute_reply").content.status == "error"

    assert handler2.counts == {"status": 2, "execute_reply": 1}
    assert handler2.get_last_msg("execute_reply").content.status == "aborted"

    assert handler3.counts == {"status": 2, "interrupt_reply": 1}


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
