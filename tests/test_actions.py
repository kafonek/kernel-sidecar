import asyncio
import collections
import textwrap
from unittest.mock import AsyncMock

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
    first_execute_action = await kernel.execute_request("import time; time.sleep(60)")
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


async def test_observer(kernel: Kernel):
    action = await kernel.execute_request("x = 1")
    cb = AsyncMock()
    obs = action.observe(cb, message_type="execute_reply", test_kwarg="test")
    await action

    assert obs.dict() == {
        "fn": cb,
        "message_type": "execute_reply",
        "kwargs": {"test_kwarg": "test"},
    }
    cb.assert_awaited_once()
    assert isinstance(cb.call_args.args[0], messages.ExecuteReplyMessage)
    assert cb.call_args.kwargs == {"test_kwarg": "test"}


async def test_no_msg_type_observer(kernel: Kernel):
    action = await kernel.execute_request("x = 1")
    cb = AsyncMock()
    action.observe(cb)
    await action

    assert cb.call_count == 4
    msg_counts = collections.defaultdict(int)
    for call in cb.call_args_list:
        msg_counts[call.args[0].msg_type] += 1
    assert msg_counts == {
        "status": 2,
        "execute_input": 1,
        "execute_reply": 1,
    }


async def test_remove_observer(kernel: Kernel):
    action = await kernel.execute_request("x = 1")
    cb = AsyncMock()
    obs = action.observe(cb)
    action.remove_observer(obs)
    await action

    cb.assert_not_awaited()


async def test_comm_open(kernel: Kernel):
    code = textwrap.dedent(
        """
    def comm_fn(comm, open_msg):
        comm.send("open")
        comm.send({'echo': open_msg['content']['data']})

    get_ipython().kernel.comm_manager.register_target("test_comm", comm_fn)
    """
    )
    execute_action = await kernel.execute_request(code)
    await execute_action
    assert execute_action.status == "ok"

    comm_open_action = await kernel.comm_open_request(target_name="test_comm", data={"test": 1})
    await comm_open_action

    assert comm_open_action.comm_id
    assert len(comm_open_action.comms) == 2
    assert comm_open_action.comms[0].data == "open"
    assert comm_open_action.comms[1].data == {"echo": {"test": 1}}


async def test_unregistered_comm(kernel: Kernel):
    comm_open_action = await kernel.comm_open_request(target_name="test_comm")
    await comm_open_action
    assert comm_open_action.error == "No such comm target registered: test_comm\n"


async def test_comm_open_error(kernel: Kernel):
    code = textwrap.dedent(
        """
    def comm_fn(comm, open_msg):
        raise ValueError("test")

    get_ipython().kernel.comm_manager.register_target("test_comm", comm_fn)
    """
    )
    execute_action = await kernel.execute_request(code)
    await execute_action
    assert execute_action.status == "ok"

    comm_open_action = await kernel.comm_open_request(target_name="test_comm")
    await comm_open_action
    assert comm_open_action.error.strip().split("\n")[-1] == "ValueError: test"


async def test_comm_msg(kernel: Kernel):
    code = textwrap.dedent(
        """
    def comm_fn(comm, open_msg):
        @comm.on_msg
        def _recv(msg):
            comm.send("msg recv")
            comm.send({'echo': msg['content']['data']})

    get_ipython().kernel.comm_manager.register_target("test_comm", comm_fn)
    """
    )
    execute_action = await kernel.execute_request(code)
    await execute_action
    assert execute_action.status == "ok"

    comm_open_action = await kernel.comm_open_request(target_name="test_comm")
    await comm_open_action

    comm_msg_action = await kernel.comm_msg_request(
        comm_id=comm_open_action.comm_id, data={"test": 1}
    )
    await comm_msg_action

    assert len(comm_msg_action.comms) == 2
    assert comm_msg_action.comms[0].data == "msg recv"
    assert comm_msg_action.comms[1].data == {"echo": {"test": 1}}


async def test_comm_msg_error(kernel: Kernel):
    code = textwrap.dedent(
        """
    def comm_fn(comm, open_msg):
        @comm.on_msg
        def _recv(msg):
            comm.send("msg recv")
            raise ValueError("test")

    get_ipython().kernel.comm_manager.register_target("test_comm", comm_fn)
    """
    )
    execute_action = await kernel.execute_request(code)
    await execute_action
    assert execute_action.status == "ok"

    comm_open_action = await kernel.comm_open_request(target_name="test_comm")
    await comm_open_action

    comm_msg_action = await kernel.comm_msg_request(comm_id=comm_open_action.comm_id)
    await comm_msg_action

    assert len(comm_msg_action.comms) == 1
    assert comm_msg_action.comms[0].data == "msg recv"
    assert comm_msg_action.error.strip().split("\n")[-1] == "ValueError: test"
