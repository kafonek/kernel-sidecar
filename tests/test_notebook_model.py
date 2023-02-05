import nbformat
from kernel_sidecar.notebook import Notebook


def test_notebook_model():
    nb = nbformat.v4.new_notebook()
    nb.cells.append(nbformat.v4.new_code_cell("1 + 1"))

    notebook = Notebook.parse_obj(nb.dict())

    assert nb.dict() == notebook.dict()
