"""
Pydantic models for an nbformat-specced Notebook.

Use:
import nbformat

nb = nbformat.v4.new_notebook()
nb.cells.append(nbformat.v4.new_code_cell("1 + 1"))

notebook = Notebook.parse_obj(nb.dict())

assert notebook.dict() == nb.dict()
"""

import uuid
from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, Field, validator

from kernel_sidecar.models import messages

CellOutput = Union[
    messages.StreamContent,
    messages.DisplayDataContent,
    messages.ExecuteResultContent,
    messages.ErrorContent,
]


# Cell types
class CellBase(BaseModel):
    """
    All Cell types have id, source and metadata.
    The source can be a string or list of strings in nbformat spec, but we only want to deal with
    source as a string throughout our code base so we have a validator here to cast the list of
    strings to a single string, both at initial read and during any updates
    """

    id: str
    source: str = ""
    metadata: dict = Field(default_factory=dict)

    @validator("source", pre=True)
    def multiline_source(cls, v):
        if isinstance(v, list):
            return "\n".join(v)
        return v

    class Config:
        validate_on_assignment = True


class CodeCell(CellBase):
    cell_type: Literal["code"] = "code"
    execution_count: Optional[int]
    outputs: List[CellOutput] = Field(default_factory=list)


class MarkdownCell(CellBase):
    cell_type: Literal["markdown"] = "markdown"


class RawCell(CellBase):
    cell_type: Literal["raw"] = "raw"


# Use: List[NotebookCell] or pydantic.parse_obj_as(NotebookCell, dict)
NotebookCell = Annotated[
    Union[
        CodeCell,
        MarkdownCell,
        RawCell,
    ],
    Field(discriminator="cell_type"),
]


class Notebook(BaseModel):
    nbformat: int = 4
    nbformat_minor: int = 5
    metadata: dict = Field(default_factory=dict)
    cells: List[NotebookCell] = Field(default_factory=list)

    @validator("cells")
    def ensure_unique_cell_ids(cls, v):
        cell_ids = []
        cleaned_cells = []
        cell: NotebookCell  # type hinting in for loop below
        for cell in v:
            if not cell.id or cell.id in cell_ids:
                cell.id = str(uuid.uuid4())
            cell_ids.append(cell.id)
            cleaned_cells.append(cell)
        return cleaned_cells
