import asyncio
import logging
import textwrap

from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.handlers.output import OutputHandler

logger = logging.getLogger(__name__)


async def test_stream(kernel: KernelSidecarClient):
    """
    Basic happy-path case of writing stream output to the document model
    """
    code = "print('foo'); print('bar')"
    cell = kernel.builder.add_cell(source=code)
    handler = OutputHandler(kernel, cell.id)
    await kernel.execute_request(cell.source, handlers=[handler])
    assert kernel.builder.nb.cells[0].outputs[0].dict() == {
        "output_type": "stream",
        "name": "stdout",
        "text": "foo\nbar\n",
    }


async def test_display_data(kernel: KernelSidecarClient):
    """
    Show that writes to the document model when getting a second display_data message that has a
    transient / display_id that we've written in another output updates both the executing cell
    output and the previous one.
    """
    builder = kernel.builder
    cell1 = builder.add_cell(source="disp = display('foo', display_id='123')")
    cell2 = builder.add_cell(source="display('bar', display_id='123')")
    cell3 = builder.add_cell(source="disp.display('baz')")
    await kernel.execute_request(cell1.source, handlers=[OutputHandler(kernel, cell1.id)])
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'foo'"}
    await kernel.execute_request(cell2.source, handlers=[OutputHandler(kernel, cell2.id)])
    assert builder.nb.cells[1].outputs[0].data == {"text/plain": "'bar'"}
    await kernel.execute_request(cell3.source, handlers=[OutputHandler(kernel, cell3.id)])
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'baz'"}


async def test_update_display_data(kernel: KernelSidecarClient):
    """
    Show that writes to the document model when receiving an update_display_data work as expected.
    """
    builder = kernel.builder
    cell1 = builder.add_cell(source="disp = display('foo', display_id='123'); disp")
    cell2 = builder.add_cell(source="disp.update('bar')")
    await kernel.execute_request(cell1.source, handlers=[OutputHandler(kernel, cell1.id)])
    await kernel.execute_request(cell2.source, handlers=[OutputHandler(kernel, cell2.id)])
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'bar'"}


async def test_clear_output(kernel: KernelSidecarClient):
    """
    Show that writes to the document model when clear_output and clear_output(wait=True) are used
    both work as expected.
    """
    await kernel.execute_request("from IPython.display import clear_output")
    builder = kernel.builder
    cell1 = builder.add_cell(source="print('foo'); clear_output()")
    cell2 = builder.add_cell(source="print('foo'); clear_output(wait=True)")
    cell3 = builder.add_cell(source="print('foo'); clear_output(wait=True); print('bar')")
    await kernel.execute_request(cell1.source, handlers=[OutputHandler(kernel, cell1.id)])
    await kernel.execute_request(cell2.source, handlers=[OutputHandler(kernel, cell2.id)])
    await kernel.execute_request(cell3.source, handlers=[OutputHandler(kernel, cell3.id)])
    assert not builder.nb.cells[0].outputs
    assert builder.nb.cells[1].outputs[0].text == "foo\n"
    assert builder.nb.cells[2].outputs[0].text == "bar\n"


async def test_output_widget(kernel: KernelSidecarClient):
    """
    Show that the OutputHandler correctly writes state to the Output widget instead of the
    document model when using "with Output()" syntax. If the NotebookBuilder has context and state
    for existing comms, it can rehydrate the document model with the widget state to replace widget
    mimetypes with the outputs stored in the widget state.
    """
    builder = kernel.builder
    code = textwrap.dedent(
        """
    from ipywidgets import Output
    out = Output()

    print('foo')
    display(out)
    print('bar')    
    """
    )
    cell1 = builder.add_cell(source=code)
    cell2 = builder.add_cell(source="with out: print('baz')")
    action1 = kernel.execute_request(cell1.source, handlers=[OutputHandler(kernel, cell1.id)])
    await asyncio.wait_for(action1, timeout=10)
    action2 = kernel.execute_request(cell2.source, handlers=[OutputHandler(kernel, cell2.id)])
    await asyncio.wait_for(action2, timeout=10)
    # While handling the "with out:" cell, OutputHandler should have sent a comm_msg back to the
    # Kernel to update the Kernel with the new Output widget state
    # So 3 actions: two execute_request, one comm_msg
    assert len(kernel.actions) == 3
    # Await all actions including the comm_msg
    await asyncio.wait_for(asyncio.gather(*kernel.actions.values()), timeout=10)

    assert builder.nb.cells[0].outputs[1].output_type == "display_data"
    assert builder.nb.cells[0].outputs[1].data["text/plain"] == "Output()"

    hydrated = builder.hydrate_output_widgets(kernel.comm_manager.comms)
    assert hydrated.cells[0].outputs[1].output_type == "stream"
    assert hydrated.cells[0].outputs[1].text == "baz\n"


async def test_error_in_output(kernel: KernelSidecarClient):
    """
    Show that the OutputHandler correctly writes state to the Output widget instead of the
    document model when using "with Output()" syntax. If the NotebookBuilder has context and state
    for existing comms, it can rehydrate the document model with the widget state to replace widget
    mimetypes with the outputs stored in the widget state.
    """
    builder = kernel.builder
    cell1 = builder.add_cell(source="from ipywidgets import Output; out = Output(); out")
    cell2 = builder.add_cell(source="with out: 1/0")
    action1 = kernel.execute_request(cell1.source, handlers=[OutputHandler(kernel, cell1.id)])
    await asyncio.wait_for(action1, timeout=10)
    logger.critical("Finished cell1")
    action2 = kernel.execute_request(cell2.source, handlers=[OutputHandler(kernel, cell2.id)])
    await asyncio.wait_for(action2, timeout=10)
    logger.critical("Finished cell2")
    # While handling the "with out:" cell, OutputHandler should have sent a comm_msg back to the
    # Kernel to update the Kernel with the new Output widget state
    # So 3 actions: two execute_request, one comm_msg
    assert len(kernel.actions) == 3
    # Await all actions including the comm_msg syncing output widget state to Kernel
    await asyncio.wait_for(asyncio.gather(*kernel.actions.values()), timeout=10)
    logger.critical("Finished all actions")

    assert builder.nb.cells[0].outputs[0].output_type == "execute_result"
    assert builder.nb.cells[0].outputs[0].data["text/plain"] == "Output()"

    hydrated = builder.hydrate_output_widgets(kernel.comm_manager.comms)
    assert hydrated.cells[0].outputs[0].output_type == "error"
    assert hydrated.cells[0].outputs[0].ename == "ZeroDivisionError"
    logger.critical("Finished test_error_in_output")
