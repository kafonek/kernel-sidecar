"""
Pydantic models for an nbformat-specced Notebook.

Use:
import nbformat

nb = nbformat.v4.new_notebook()
nb.cells.append(nbformat.v4.new_code_cell("1 + 1"))

notebook = Notebook.validate_python(nb.model_dump())

assert notebook.model_dump() == nb.model_dump()
"""

import uuid
from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

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

    @field_validator("source", mode="before")
    @classmethod
    def multiline_source(cls, v):
        if isinstance(v, list):
            return "\n".join(v)
        return v
    model_config = ConfigDict(validate_on_assignment=True)


class CodeCell(CellBase):
    cell_type: Literal["code"] = "code"
    execution_count: Optional[int] = None
    outputs: List[CellOutput] = Field(default_factory=list)


class MarkdownCell(CellBase):
    cell_type: Literal["markdown"] = "markdown"


class RawCell(CellBase):
    cell_type: Literal["raw"] = "raw"


# Use: List[NotebookCell] or TypeAdapter(NotebookCell).validate_python(data)
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

    @field_validator("cells")
    @classmethod
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
