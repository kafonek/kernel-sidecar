import asyncio
import textwrap

import pytest
from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.models.notebook import Notebook
from kernel_sidecar.nb_builder import NotebookBuilder, SimpleOutputHandler


@pytest.fixture
def builder() -> NotebookBuilder:
    return NotebookBuilder(nb=Notebook())


async def test_stream(kernel: KernelSidecarClient, builder: NotebookBuilder):
    """
    Basic happy-path case of writing stream output to the document model
    """
    code = "print('foo'); print('bar')"
    cell = builder.add_cell(source=code)
    handler = SimpleOutputHandler(kernel, cell.id, builder)
    await kernel.execute_request(cell.source, handlers=[handler])
    assert builder.nb.cells[0].outputs[0].dict() == {
        "output_type": "stream",
        "name": "stdout",
        "text": "foo\nbar\n",
    }
    assert builder.nb.cells[0].execution_count == 1


async def test_display_data(kernel: KernelSidecarClient, builder: NotebookBuilder):
    """
    Show that display_data syncs are called when:
     - another cell has display() with same display_id
     - another cell calls disp.display()
     - another cell calls disp.update()
    """
    cell1 = builder.add_cell(source="disp = display('foo', display_id='123')")
    cell2 = builder.add_cell(source="display('bar', display_id='123')")
    cell3 = builder.add_cell(source="disp.display('baz')")
    cell4 = builder.add_cell(source="disp.update('qux')")
    await kernel.execute_request(
        cell1.source, handlers=[SimpleOutputHandler(kernel, cell1.id, builder)]
    )
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'foo'"}
    await kernel.execute_request(
        cell2.source, handlers=[SimpleOutputHandler(kernel, cell2.id, builder)]
    )
    # should have updated first cell since they have the same display id
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'bar'"}
    assert builder.nb.cells[1].outputs[0].data == {"text/plain": "'bar'"}
    await kernel.execute_request(
        cell3.source, handlers=[SimpleOutputHandler(kernel, cell3.id, builder)]
    )
    # calling `.display()` emits a display_data, so we should see cell3 have an output even though
    # disp isn't "returned" (no execute_result). Also should sync the first two cell displays.
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'baz'"}
    assert builder.nb.cells[1].outputs[0].data == {"text/plain": "'baz'"}
    assert builder.nb.cells[2].outputs[0].data == {"text/plain": "'baz'"}
    await kernel.execute_request(
        cell4.source, handlers=[SimpleOutputHandler(kernel, cell4.id, builder)]
    )
    # calling `.update()` emits an update_display_data, but no display_data or execute_result, so
    # cell4 should have no outputs while the first three cells all have updated display content
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'qux'"}
    assert builder.nb.cells[1].outputs[0].data == {"text/plain": "'qux'"}
    assert builder.nb.cells[2].outputs[0].data == {"text/plain": "'qux'"}
    assert builder.nb.cells[3].outputs == []


async def test_clear_output(kernel: KernelSidecarClient, builder: NotebookBuilder):
    """
    Show that writes to the document model when clear_output and clear_output(wait=True) are used
    both work as expected.
    """
    await kernel.execute_request("from IPython.display import clear_output")
    cell1 = builder.add_cell(source="print('foo'); clear_output()")
    cell2 = builder.add_cell(source="print('foo'); clear_output(wait=True)")
    cell3 = builder.add_cell(source="print('foo'); clear_output(wait=True); print('bar')")
    await kernel.execute_request(
        cell1.source, handlers=[SimpleOutputHandler(kernel, cell1.id, builder)]
    )
    await kernel.execute_request(
        cell2.source, handlers=[SimpleOutputHandler(kernel, cell2.id, builder)]
    )
    await kernel.execute_request(
        cell3.source, handlers=[SimpleOutputHandler(kernel, cell3.id, builder)]
    )
    assert not builder.nb.cells[0].outputs
    assert builder.nb.cells[1].outputs[0].text == "foo\n"
    assert builder.nb.cells[2].outputs[0].text == "bar\n"


async def test_output_widget(kernel: KernelSidecarClient, builder: NotebookBuilder):
    """
    Show that we're handling the "widget sandwich" Output widget pattern correctly. When a cell
    enters the Output widget context "with out:", we should send state back to the Kernel via comm,
    write the widget mimetype to the document model, and store output widget state in the builder.
    If we want to update the output widget mimetypes to state, we can "hydrate" the document model.
    """
    # basic stream output
    code = textwrap.dedent(
        """
    from ipywidgets import Output
    out = Output()
    out
    """
    )
    cell1 = builder.add_cell(source=code)
    cell2 = builder.add_cell(source="with out: print('baz')")
    await kernel.execute_request(
        cell1.source, handlers=[SimpleOutputHandler(kernel, cell1.id, builder)]
    )
    await kernel.execute_request(
        cell2.source, handlers=[SimpleOutputHandler(kernel, cell2.id, builder)]
    )
    # While handling the "with out:" cell, OutputHandler should have sent a comm_msg back to the
    # Kernel to update the Kernel with the new Output widget state
    # So 3 actions: two execute_request, one comm_msg
    assert len(kernel.actions) == 3
    # Await all actions including the comm_msg
    import logging

    logger = logging.getLogger()
    logger.info("{kernel.actions.values()=}}")
    await asyncio.wait_for(asyncio.gather(*kernel.actions.values()), timeout=3)

    assert builder.nb.cells[0].outputs[0].output_type == "execute_result"
    assert builder.nb.cells[0].outputs[0].data["text/plain"] == "Output()"

    hydrated = builder.hydrate_output_widgets()
    assert hydrated.cells[0].outputs[0].output_type == "stream"
    assert hydrated.cells[0].outputs[0].text == "baz\n"

    # test clear_output and error output
    code = textwrap.dedent(
        """
    from IPython.display import clear_output
    with out: 
        clear_output()
        1 / 0
        
    """
    )
    cell3 = builder.add_cell(source=code)
    await kernel.execute_request(
        cell3.source, handlers=[SimpleOutputHandler(kernel, cell3.id, builder)]
    )
    assert builder.nb.cells[0].outputs[0].output_type == "execute_result"
    assert builder.nb.cells[0].outputs[0].data["text/plain"] == "Output()"
    assert builder.nb.cells[2].outputs == []

    hydrated = builder.hydrate_output_widgets()
    assert hydrated.cells[0].outputs[0].output_type == "error"
    assert hydrated.cells[0].outputs[0].ename == "ZeroDivisionError"

    # Prove that we've been syncing state to the kernel-side widget
    cell4 = builder.add_cell(source="out.outputs")
    await kernel.execute_request(
        cell4.source, handlers=[SimpleOutputHandler(kernel, cell4.id, builder)]
    )
    assert builder.nb.cells[3].outputs[0].output_type == "execute_result"
    assert "ZeroDivisionError" in builder.nb.cells[3].outputs[0].data["text/plain"]
