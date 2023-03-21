"""
A composition-style tool for an Execute Request Handler to use for updating outputs when code cells
are run for a Notebook. The primary edge cases being handled here are updating `display_data` and
Output widgets that may need to be rerendered or updated.
"""
import logging
import uuid
from typing import Dict, Optional, Union

import pydantic

from kernel_sidecar.models import messages, notebook

logger = logging.getLogger(__name__)


class NotebookBuilder:
    def __init__(self, nb: notebook.Notebook):
        self.nb = nb
        self.display_ids: Dict[str, messages.DisplayDataContent] = {}

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

    def add_output(
        self,
        cell_id: str,
        content: Union[
            messages.ExecuteResultContent,
            messages.StreamContent,
            messages.ErrorContent,
            messages.DisplayDataContent,
        ],
    ):
        cell = self.get_cell(cell_id)
        if not cell:
            logger.warning(f"Cell not found: {cell_id}")
            return
        cell.outputs.append(content)

    def clear_output(self, cell_id: str):
        cell = self.get_cell(cell_id)
        if not cell:
            logger.warning(f"Cell not found: {cell_id}")
            return
        cell.outputs = []

    def replace_display_data(
        self, content: Union[messages.DisplayDataContent, messages.UpdateDisplayDataContent]
    ):
        self.display_ids[content.display_id] = content
        for cell in self.nb.cells:
            for idx, output in enumerate(cell.outputs):
                if isinstance(output, messages.DisplayDataContent):
                    if output.display_id == content.display_id:
                        cell.outputs[idx] = content
