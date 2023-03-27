"""
A composition-style tool for an Execute Request Handler to use for updating outputs when code cells
are run for a Notebook. The primary edge cases being handled here are updating `display_data` and
Output widgets that may need to be rerendered or updated.
"""
import logging
import uuid
from typing import Dict, List, Optional, Union

import pydantic

from kernel_sidecar.models import messages, notebook

logger = logging.getLogger(__name__)

ContentType = Union[
    messages.ExecuteResultContent,
    messages.StreamContent,
    messages.ErrorContent,
    messages.DisplayDataContent,
]


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

    def add_cell_output(
        self,
        cell_id: str,
        content: ContentType,
    ):
        cell = self.get_cell(cell_id)
        if not cell:
            logger.warning(f"Cell not found: {cell_id}")
            return
        cell.outputs.append(content)

    def clear_cell_output(self, cell_id: str):
        cell = self.get_cell(cell_id)
        if not cell:
            logger.warning(f"Cell not found: {cell_id}")
            return
        cell.outputs = []

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
