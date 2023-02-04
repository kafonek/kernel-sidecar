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
    assert action.outputs[0].output_type == "execute_result"
    assert action.outputs[0].data == {"text/plain": "2"}
    assert action.outputs[0].execution_count == 1


async def test_execute_stream(kernel: Kernel):
    action = await kernel.execute_request("print('hello')")

    assert isinstance(action, actions.ExecuteAction)
    assert action.msg_id
    assert action.request_content.code == "print('hello world')"

    await action

    assert action.status == "ok"
    assert len(action.outputs) == 1
    assert action.outputs[0].output_type == "stream"
    assert action.outputs[0].name == "stdout"
    assert len(action.outputs[0].text) == 1
    assert action.outputs[0].text[0] == "hello world\n"
