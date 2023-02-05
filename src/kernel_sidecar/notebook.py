"""
Pydantic models for an nbformat-specced Notebook.

Use:
import nbformat

nb = nbformat.v4.new_notebook()
nb.cells.append(nbformat.v4.new_code_cell("1 + 1"))

notebook = Notebook.parse_obj(nb.dict())

assert notebook.dict() == nb.dict()
"""

from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, validator


# Cell outputs modeled with a discriminator pattern where the output_type
# field will determine what kind of output we have
# https://nbformat.readthedocs.io/en/latest/format_description.html#code-cell-outputs
class StreamOutput(BaseModel):
    output_type: Literal["stream"] = "stream"
    name: str  # stdout or stderr
    text: str


class DisplayDataOutput(BaseModel):
    output_type: Literal["display_data"] = "display_data"
    data: Dict[str, Any]
    metadata: Dict[str, Any]


class ExecuteResultOutput(BaseModel):
    output_type: Literal["execute_result"] = "execute_result"
    execution_count: int
    data: Dict[str, Any]
    metadata: Dict[str, Any]


class ErrorOutput(BaseModel):
    output_type: Literal["error"] = "error"
    ename: str
    evalue: str
    traceback: List[str]


# Use: List[CellOutput] or pydantic.parse_obj_as(CellOutput, dict)
CellOutput = Annotated[
    Union[StreamOutput, DisplayDataOutput, ExecuteResultOutput, ErrorOutput],
    Field(discriminator="output_type"),
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
