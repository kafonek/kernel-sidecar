import nbclient
import nbformat
from kernel_sidecar.models.notebook import Notebook


def test_notebook_model():
    """
    Test that we are not losing fields when converting from nbformat dict to our
    Pydantic Notebook model.

    Use nbclient.NotebookClient to actually execute the code so Notebook metadata,
    cell metadata, and cell output are all present.
    """
    nb = nbformat.v4.new_notebook()
    nb.cells.append(nbformat.v4.new_code_cell("print('hello world')"))
    nb.cells.append(nbformat.v4.new_code_cell("1 + 1"))

    client = nbclient.NotebookClient(nb)
    client.execute()

    notebook = Notebook.parse_obj(nb.dict())

    assert nb.dict() == notebook.dict()
