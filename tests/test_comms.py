import textwrap

import pytest
from kernel_sidecar.client import CommTargetNotFound, KernelSidecarClient
from kernel_sidecar.comms import CommHandler
from kernel_sidecar.handlers.debug import DebugHandler
from kernel_sidecar.models import messages


class DebugCommHandler(CommHandler, DebugHandler):
    """
    DebugHandler / CommHandler mixin. Use:

    (assuming 'foo' comm target_name is registered on kernel side and will echo comm_msg data back)

    debug_comm_handler = await kernel.comm_open('foo')
    action = kernel.comm_msg_request(debug_comm_handler.comm_id, data={"foo": "bar"})
    assert debug_comm_handler.counts == {"comm_msg": 1}
    assert debug_comm_handler.get_last_msg("comm_msg").content.data == {"foo": "bar"}
    """

    def __init__(self, comm_id: str):
        CommHandler.__init__(self, comm_id)
        DebugHandler.__init__(self)


async def test_comm_target_not_found(kernel: KernelSidecarClient):
    """
    Show that if the kernel tries to open a comm without a comm being registered in the kernel by
    target_name / comm_id, it will raise CommTargetNotFound
    """
    with pytest.raises(CommTargetNotFound):
        await kernel.comm_open(target_name="foo", handler_cls=CommHandler)


async def test_comm_happy_path(kernel: KernelSidecarClient):
    """
    This is the "normal" way we expect to use Comms in a sidecar application. A comm target is
    registered on the Kernel side, but not opened yet. The reason this to do it this way is that
    you'll get a direct reference to the CommHandler instance (with comm_id) in the sidecar compute
    when opening from sidecar, and the syntax to send messages after that is simple.

    If the comm is opened from the kernel side, we'll still have a CommHandler instance on the
    sidecar side but would need some kind of logic to look it up in kernel.comm_manager.comms.
    """
    # register comm on kernel side first, it will emit a comm_msg on comm open
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
    await kernel.execute_request(code)

    # - instantiates a DebugCommHandler class and registers it with the kernel.comm_manager
    # - sends comm_open request to kernel and watches for errors during that request-reply
    # - no errors since target_name is registered earlier, return DebugCommHandler instance
    comm_handler: DebugCommHandler = await kernel.comm_open(
        target_name="test_comm", handler_cls=DebugCommHandler
    )
    assert comm_handler.comm_id

    assert comm_handler.counts == {"comm_msg": 1}
    comm_msg: messages.CommMsg = comm_handler.get_last_msg("comm_msg")
    assert comm_msg.content.data == "connected"

    await kernel.comm_msg_request(comm_handler.comm_id, data={"foo": "bar"})
    assert comm_handler.counts == {"comm_msg": 2}
    comm_msg: messages.CommMsg = comm_handler.get_last_msg("comm_msg")
    assert comm_msg.content.data == {"echo": {"foo": "bar"}}


async def test_kernel_open_comm(kernel: KernelSidecarClient):
    """
    Show how to handle cases where the kernel opens comms, such as when you create an ipywidget
    in the kernel. Usually these comm_open messages come in as replies to an execute_requset,
    but they could be a side effect of handling a comm_msg or something else. Either way, the
    kernel.comm_manager as a handler is attached to every request, so we don't need to worry about
    catching the comm_open in explicit handlers attached to an action.

    When comm_manager handler sees the comm_open, it checks its comm_manager.handlers dictionary
    to see if it has a class for the comm_open's target_name. If so, instantiate it and add to
    comm_manager.comms dictionary.

    comm_* messages are delegated to the CommHandler instance, and also seen by any handlers
    watching the execute_request request-reply action.
    """
    # "factory pattern" - register a class that will be instantiated when a comm_open comes in
    # for the given target_name
    kernel.comm_manager.handlers["test_comm"] = DebugCommHandler
    # handler to watch replies just for this execute_request
    msg_handler = DebugHandler()
    code = textwrap.dedent(
        """
    from ipykernel.comm import Comm
    comm = Comm(target_name="test_comm", data="connected")
    comm.send({"hello": "world"})
    comm.comm_id
    """
    )
    action = kernel.execute_request(code, handlers=[msg_handler])
    await action
    assert msg_handler.counts == {
        "status": 2,
        "execute_input": 1,
        "comm_open": 1,
        "comm_msg": 1,
        "execute_result": 1,
        "execute_reply": 1,
    }
    execute_result: messages.ExecuteResult = msg_handler.get_last_msg("execute_result")
    comm_id = execute_result.content.data["text/plain"].strip("'")

    assert len(kernel.comm_manager.comms) == 1
    comm_handler: DebugCommHandler = kernel.comm_manager.comms[comm_id]
    assert comm_handler.counts == {"comm_open": 1, "comm_msg": 1}

    comm_open: messages.CommOpen = comm_handler.get_last_msg("comm_open")
    assert comm_open.content.data == "connected"

    comm_msg: messages.CommMsg = comm_handler.get_last_msg("comm_msg")
    assert comm_msg.content.data == {"hello": "world"}


async def test_ipywidgets(kernel: KernelSidecarClient):
    """
    Show how to capture comm messages emitted by ipywidgets. Future work in kernel-sidecar might
    add a default jupyter.widget CommHandler, but for now just show the counts of comm_open and
    comm_msg's that are seen when creating, updating, and rendering an ipywidget kernel side.
    """
    kernel.comm_manager.handlers["jupyter.widget"] = DebugCommHandler
    msg_handler = DebugHandler()
    code = textwrap.dedent(
        """
    from ipywidgets import IntSlider
    slider = IntSlider()
    slider.value = 5
    slider
    """
    )
    action = kernel.execute_request(code, handlers=[msg_handler])
    await action
    assert msg_handler.counts == {
        "status": 2,
        "execute_input": 1,
        "comm_open": 3,
        "comm_msg": 1,
        "execute_result": 1,
        "execute_reply": 1,
    }
    execute_result: messages.ExecuteResult = msg_handler.get_last_msg("execute_result")
    assert execute_result.content.data["text/plain"] == "IntSlider(value=5)"
    # ipywidgets opens three comms: one for Layout, one for Style, and one for widget value
    assert len(kernel.comm_manager.comms) == 3


async def test_comm_msg_side_effects(kernel: KernelSidecarClient):
    """
    Highlight an edge case here that is not necessarily well handled in regular Jupyter / Lab,
    what happens if a comm_msg function kernel side ends up emitting `stream` or other message
    types? In kernel-sidecar, just show that we can still capture those with a handler attached
    to the comm_msg request Action, while also guarenteeing any comm_msg automatically get caught
    by comm_manager and delegated to our comm_handler.
    """
    # When we comm_open to target name test_comm, then send a comm_msg by comm_id afterwards,
    # we should see a stream, update_display_data, and comm_msg back
    code = textwrap.dedent(
        """
    from IPython.display import display

    disp = display("foo", display_id=123)

    def comm_fn(comm, open_msg):
        @comm.on_msg
        def _recv(msg):
            print("test")
            disp.update("bar")
            comm.send({"echo": msg["content"]["data"]})

    get_ipython().kernel.comm_manager.register_target("test_comm", comm_fn)
    """
    )
    # register the comm kernel side
    await kernel.execute_request(code)

    # send comm_open and get our instantiated CommHandler
    comm_handler: DebugCommHandler = await kernel.comm_open(
        target_name="test_comm", handler_cls=DebugCommHandler
    )

    # now send comm_msg, attach DebugHandler explicitly to it. Assert we see messages on the
    # DebugHandler, and that we also see messages in from comm_handler
    comm_handler.counts.clear()  # reset counts, only want to assert what's seen after this comm_msg
    msg_handler = DebugHandler()
    action = kernel.comm_msg_request(
        comm_id=comm_handler.comm_id, data={"foo": "bar"}, handlers=[msg_handler]
    )
    await action

    assert msg_handler.counts == {
        "status": 2,
        "comm_msg": 1,
        "stream": 1,
        "update_display_data": 1,
    }

    assert comm_handler.counts == {"comm_msg": 1}
    comm_msg: messages.CommMsg = comm_handler.get_last_msg("comm_msg")
    assert comm_msg.content.data == {"echo": {"foo": "bar"}}


async def test_inter_comm_msgs(kernel: KernelSidecarClient):
    """
    Show that if one Comm sends messages out to a different Comm, we end up routing those to the
    right CommHandler's on the kernel side. In the test below, even though comm_handler1 sends
    the comm_msg that is getting acted on, the kernel is emitting a comm_msg with the comm_id for
    our comm_handler2, so we should only expect that comm_handler2 has received a message to handle.
    """
    # Set it up so that when a comm_msg is handled by one comm, it broadcasts that message to
    # all other comms
    code = textwrap.dedent(
        """
    all_comms = []
    def comm_fn(comm, open_msg):
        @comm.on_msg
        def _recv(msg):
            for other_comm in all_comms:
                if other_comm.comm_id != comm.comm_id:
                    other_comm.send({"broadcasting": msg["content"]["data"]})
            

        all_comms.append(comm)

    get_ipython().kernel.comm_manager.register_target("test_comm", comm_fn)
    """
    )
    await kernel.execute_request(code)

    comm_handler1: DebugCommHandler = await kernel.comm_open(
        target_name="test_comm", handler_cls=DebugCommHandler
    )
    comm_handler2: DebugCommHandler = await kernel.comm_open(
        target_name="test_comm", handler_cls=DebugCommHandler
    )

    await kernel.comm_msg_request(comm_id=comm_handler1.comm_id, data={"foo": "bar"})
    assert comm_handler1.counts == {}
    assert comm_handler2.counts == {"comm_msg": 1}
    comm_msg: messages.CommMsg = comm_handler2.get_last_msg("comm_msg")
    assert comm_msg.content.data == {"broadcasting": {"foo": "bar"}}
