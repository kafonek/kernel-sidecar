import pytest
from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.handlers import OutputHandler
from kernel_sidecar.models.notebook import Notebook
from kernel_sidecar.nb_builder import NotebookBuilder


@pytest.fixture
def builder() -> NotebookBuilder:
    return NotebookBuilder(nb=Notebook())


async def test_stream(kernel: KernelSidecarClient, builder: NotebookBuilder):
    code = "print('foo'); print('bar')"
    cell = builder.add_cell(source=code)
    handler = OutputHandler(cell.id, builder)
    await kernel.execute_request(cell.source, handlers=[handler])
    assert builder.nb.cells[0].outputs[0].dict() == {
        "output_type": "stream",
        "name": "stdout",
        "text": "foo\nbar\n",
    }


async def test_display_data(kernel: KernelSidecarClient, builder: NotebookBuilder):
    cell1 = builder.add_cell(source="display('foo', display_id='123')")
    cell2 = builder.add_cell(source="display('bar', display_id='123')")
    await kernel.execute_request(cell1.source, handlers=[OutputHandler(cell1.id, builder)])
    await kernel.execute_request(cell2.source, handlers=[OutputHandler(cell2.id, builder)])
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'bar'"}


async def test_update_display_data(kernel: KernelSidecarClient, builder: NotebookBuilder):
    cell1 = builder.add_cell(source="disp = display('foo', display_id='123'); disp")
    cell2 = builder.add_cell(source="disp.update('bar')")
    await kernel.execute_request(cell1.source, handlers=[OutputHandler(cell1.id, builder)])
    await kernel.execute_request(cell2.source, handlers=[OutputHandler(cell2.id, builder)])
    assert builder.nb.cells[0].outputs[0].data == {"text/plain": "'bar'"}
