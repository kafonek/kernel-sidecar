"""
A composition-style tool for an Execute Request Handler to use for updating outputs when code cells
are run for a Notebook. The primary edge cases being handled here are updating `display_data` and
Output widgets that may need to be rerendered or updated.
"""
import logging
import uuid
from typing import Dict, List, Optional, Union

import pydantic

from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.comms import WidgetHandler
from kernel_sidecar.handlers.output import ContentType, OutputHandler
from kernel_sidecar.models import messages, notebook

logger = logging.getLogger(__name__)


class NotebookBuilder:
    def __init__(self, nb: notebook.Notebook):
        self.nb = nb
        self.output_widget_state: Dict[str, List[ContentType]] = {}

    def get_cell(self, cell_id: str) -> Optional[notebook.NotebookCell]:
        for cell in self.nb.cells:
            if cell.id == cell_id:
                return cell

    def add_cell(self, source: str = "", id: Optional[str] = None, cell_type: str = "code"):
        data = {"id": id or str(uuid.uuid4()), "source": source, "cell_type": cell_type}
        if self.get_cell(data["id"]):
            data["id"] = str(uuid.uuid4())
        cell = pydantic.parse_obj_as(notebook.NotebookCell, data)
        self.nb.cells.append(cell)
        return cell

    def add_cell_output(self, cell_id: str, content: ContentType):
        cell = self.get_cell(cell_id)
        if not cell:
            logger.warning(f"Cell not found: {cell_id}")
            return
        cell.outputs.append(content)

    def set_execution_count(self, cell_id: str, execution_count: int):
        cell = self.get_cell(cell_id)
        if not cell:
            logger.warning(f"Cell not found: {cell_id}")
            return
        if cell.cell_type != "code":
            logger.warning(f"Cell is not a code cell: {cell_id}")
            return
        cell.execution_count = execution_count

    def clear_cell_output(self, cell_id: str):
        cell = self.get_cell(cell_id)
        if not cell:
            logger.warning(f"Cell not found: {cell_id}")
            return
        cell.outputs = []
        if cell.cell_type == "code":
            cell.execution_count = None

    def replace_display_data(
        self, content: Union[messages.DisplayDataContent, messages.UpdateDisplayDataContent]
    ):
        for cell in self.nb.cells:
            for idx, output in enumerate(cell.outputs):
                if isinstance(output, messages.DisplayDataContent):
                    if output.display_id == content.display_id:
                        cell.outputs[idx] = content

    def hydrate_output_widgets(self) -> notebook.Notebook:
        widget_mimetype = "application/vnd.jupyter.widget-view+json"
        cleaned = self.nb.copy(deep=True)
        for cell in cleaned.cells:
            for idx, output in enumerate(cell.outputs):
                if isinstance(output, (messages.DisplayDataContent, messages.ExecuteResultContent)):
                    if widget_mimetype in output.data:
                        comm_id = output.data[widget_mimetype]["model_id"]
                        if comm_id not in self.output_widget_state:
                            logger.warning(
                                f"Output widget {comm_id} listed in output but no state cached"
                            )
                        else:
                            output_state = self.output_widget_state[comm_id]
                            cell.outputs.pop(idx)
                            for content in output_state:
                                cell.outputs.insert(idx, content)
                                idx += 1
        return cleaned


class SimpleOutputHandler(OutputHandler):
    """
    Update a NotebookBuilder instance while handling execute_request responses. See the
    NotebookBuilder class for moe details, but in a nutshell it updates the Pydantic-modeled
    in-memory Notebook document when adding/clearing content or syncing display data, as well as
    keeping Output widget state to hydrate Output widgets in display_data/execute_result content
    to replace the Output widget mimetype with actual output content.
    """

    def __init__(self, client: KernelSidecarClient, cell_id: str, builder: NotebookBuilder):
        super().__init__(client, cell_id)
        self.builder = builder

    async def add_cell_content(self, content: ContentType):
        self.builder.add_cell_output(self.cell_id, content)

    async def clear_cell_content(self):
        self.builder.clear_cell_output(self.cell_id)

    async def add_output_widget_content(self, handler: WidgetHandler, content: ContentType):
        self.builder.output_widget_state[handler.comm_id] = handler.state["outputs"]

    async def clear_output_widget_content(self, handler: WidgetHandler):
        self.builder.output_widget_state[handler.comm_id] = handler.state["outputs"]

    async def sync_display_data(
        self, content: Union[messages.DisplayDataContent, messages.UpdateDisplayDataContent]
    ):
        self.builder.replace_display_data(content)

    async def handle_execute_input(self, msg: messages.ExecuteInput):
        self.builder.set_execution_count(self.cell_id, msg.content.execution_count)
