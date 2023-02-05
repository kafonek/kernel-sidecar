import asyncio

from kernel_sidecar import Kernel, actions, messages


async def test_kernel_info(kernel: Kernel):
    action = await kernel.kernel_info_request()

    assert isinstance(action, actions.KernelInfoAction)
    assert action.msg_id
    assert not action.content

    await action

    assert isinstance(action.content, messages.KernelInfoReplyContent)
    assert action.content.status == "ok"


async def test_execute_statement(kernel: Kernel):
    action = await kernel.execute_request("1 + 1")

    assert isinstance(action, actions.ExecuteAction)
    assert action.msg_id
    assert action.request_content.code == "1 + 1"

    await action

    assert action.status == "ok"
    assert len(action.outputs) == 1
    assert action.outputs[0].dict() == {
        "output_type": "execute_result",
        "data": {"text/plain": "2"},
        "metadata": {},
        "execution_count": 1,
    }


async def test_execute_stream(kernel: Kernel):
    action = await kernel.execute_request("print('hello world')")

    assert isinstance(action, actions.ExecuteAction)
    assert action.msg_id
    assert action.request_content.code == "print('hello world')"

    await action

    assert action.status == "ok"
    assert len(action.outputs) == 1
    assert action.outputs[0].dict() == {
        "output_type": "stream",
        "name": "stdout",
        "text": "hello world\n",
    }


async def test_display_data(kernel: Kernel):
    action = await kernel.execute_request('display("hello world")')

    assert isinstance(action, actions.ExecuteAction)
    assert action.msg_id
    assert action.request_content.code == 'display("hello world")'

    await action

    assert action.status == "ok"
    assert len(action.outputs) == 1
    assert action.outputs[0].dict() == {
        "output_type": "display_data",
        "data": {"text/plain": "'hello world'"},
        "metadata": {},
        "transient": {},
    }


async def test_display_with_id(kernel: Kernel):
    action = await kernel.execute_request('display("hello world", display_id="test")')

    assert isinstance(action, actions.ExecuteAction)
    assert action.msg_id
    assert action.request_content.code == 'display("hello world", display_id="test")'

    await action

    assert action.status == "ok"
    assert len(action.outputs) == 2
    assert action.outputs[0].dict() == {
        "output_type": "display_data",
        "data": {"text/plain": "'hello world'"},
        "metadata": {},
        "transient": {"display_id": "test"},
    }
    assert action.outputs[1].dict() == {
        "output_type": "execute_result",
        "data": {"text/plain": "<DisplayHandle display_id=test>"},
        "execution_count": 1,
        "metadata": {},
    }


async def test_update_display(kernel: Kernel):
    code = """
    disp = display("hello world", display_id="test")
    disp.update("nice to meet you")
    """
    action = await kernel.execute_request(code)

    assert isinstance(action, actions.ExecuteAction)
    assert action.msg_id
    assert action.request_content.code == code

    await action

    assert action.status == "ok"
    assert len(action.outputs) == 2
    assert action.outputs[0].dict() == {
        "output_type": "display_data",
        "data": {"text/plain": "'hello world'"},
        "metadata": {},
        "transient": {"display_id": "test"},
    }
    assert action.outputs[1].dict() == {
        "output_type": "update_display_data",
        "data": {"text/plain": "'nice to meet you'"},
        "metadata": {},
        "transient": {"display_id": "test"},
    }


async def test_interrupt(kernel: Kernel):
    first_execute_action = await kernel.execute_request("import time; time.sleep(10)")
    second_execute_action = await kernel.execute_request("1 + 1")
    interrupt_action = await kernel.interrupt_request()

    assert isinstance(first_execute_action, actions.ExecuteAction)
    assert isinstance(second_execute_action, actions.ExecuteAction)
    assert isinstance(interrupt_action, actions.InterruptAction)

    await asyncio.gather(first_execute_action, interrupt_action, second_execute_action)

    assert first_execute_action.status == "error"
    assert first_execute_action.error.ename == "KeyboardInterrupt"

    assert second_execute_action.status == "aborted"


async def test_complete(kernel: Kernel):
    execute_action = await kernel.execute_request("x = 1")
    await execute_action

    complete_action = await kernel.complete_request("x.")
    await complete_action

    assert complete_action.content.status == "ok"
    expected_matches = [meth for meth in dir(int) if not meth.startswith("_")]
    assert complete_action.content.matches == expected_matches
