<p align="center">
Kernel Sidecar
</p>

<p align="center">
<img alt="Pypi" src="https://img.shields.io/pypi/v/kernel-sidecar">
<a href="https://github.com/kafonek/kernel-sidecar/actions/workflows/tests.yaml">
    <img src="https://github.com/kafonek/kernel-sidecar/actions/workflows/tests.yaml/badge.svg" alt="Tests" />
</a>
<img alt="Python versions" src="https://img.shields.io/pypi/pyversions/kernel-sidecar">
</p>

# Kernel-Sidecar

This package offers the building blocks for creating a "Kernel Sidecar" Jupyter framework. In normal Jupyter Notebook architecture, one or many frontends manage the document model (code cells, outputs, metadata, etc) and send requests to a single Kernel. Each frontend observes responses on different ZMQ channels (`iopub`, `shell`, etc) but may end up with some inconsistency based on the Kernel only sending certain responses to the client that made the request.

In a `kernel-sidecar` architecture, all frontend clients talk to the `kernel-sidecar` client, and only the `kernel-sidecar` client communicates with the Kernel over ZMQ. That pattern offers several potential features:
 - Keep a document model within `kernel-sidecar` or the backend architecture
 - Add "extension"-esque capabilities on the backend such as auto-linting code on execute
 - Eliminate inconsistencies in what messages individual frontends receive because of Kernel replies
 - Model all requests, replies, and the Notebook document with Pydantic

## Installation

```bash
pip install kernel-sidecar
```

# Key Concepts
## KernelSidecarClient

A manager that uses `jupyter_client` under the hood to create ZMQ connections and watch for messages coming in over different ZMQ channels (`iopub`, `shell`, etc. An important assumption here is that `kernel-sidecar` is the only client talking to the Kernel, which means every message observed coming from the Kernel should be a reply (based on `parent_header_msg.msg_id`) to a request sent from this client.

When the `KernelSidecarClient` send a request to the Kernel, it is wrapped in an `KernelAction` class. Every message received from the Kernel is delegated to the requesting Action and triggers callbacks attached to the Action class.

## Actions

Actions in `kernel-sidecar` encompass a request-reply cycle, including an `await action` syntax, where the Action is complete when the Kernel has reported its status returning to `idle` and optionally emitted a reply appropriate for the request. For instance, an `execute_request` is "done" when the `status` has been reported as `idle` *and* the Kernel has emitted an `execute_reply`, both with the `parent_header_msg.msg_id` the same as the `execute_request` `header.msg_id`.

In a nutshell, an `actions.KernelAction` takes in a `requests.Request` and zero-to-many `handlers.Handler` subclasses (or just `async functions`) and creates an `awaitable` instance. `kernel.send(action)` submits the Request over ZMQ, and registers the Action so that all observed messages get routed to that Action to be handled by the Handlers/callbacks.

Most of the time, you should be able to just use convience functions in the `KernelSidecarClient` class to create the actions. See `tests/test_actions.py` for many examples of using Actions and Handlers.

## Handlers

When the `KernelSidecarClient` receives a message over ZMQ, parses it into a Pydantic model, and delegates it to the appropriate `Action` to be handled, it passes on that message to every `Handler` attached to the `Action` and awaits all of them to handle that message. `Handler` objects can define handling different message types by creating methods `handle_<msg_type>`. See `handlers.DebugHandler` or `cli.OutputHandler` for examples of custom Handlers.

## Comms

Comms are a flexible way for a client and the Kernel to send messages outside of the `execute_request` format. The most widely used package that utilizes Comms is probably `ipywidgets`, but Comms in general are a very powerful tool for a Sidecar application. A Comm can be opened by either the Sidecar or the Kernel. A target for that Comm should be registered on the other side before the open happens. It's probably most typical to register a Comm target in the Kernel by sending an `execute_request`, then sending a `comm_open` from the Sidecar side. See `tests/test_comms.py` for examples.

Once a Comm is open, it has a unique `comm_id`. `KernelSidecarClient` will automatically route all `comm_msg` messages to a `CommHandler` instance by `comm_id` in the `comm_msg` content. That routing pattern is a bit confusing as it overlaps the `Handler` / `Action` pattern, but it's necessary because `comm_msg` can come in as a result of `execute_request`'s or `comm_msg`'s or potentially other messages. So the `CommManager` -> `CommHandler` routing basically needs to be applied to every message the `KernelSidecarClient` receives over ZMQ.


## Models

`kernel-sidecar` has Pydantic models for:
 - The Jupyter Notebook document (`models/notebook.py`), which should be consistent with `nbformat` parsing / structure
 - Request messages sent to the Kernel over ZMQ (`models/requests.py`)
 - Messages received over ZMQ from the Kernel (`models/messages.py`)


## CLI

`kernel-sidecar` ships a small CLI for testing a connection to a Kernel.

```bash
❯ sidecar --help
Usage: sidecar [OPTIONS]

Options:
  -f FILE                         Kernel connection file  [required]
  --debug / --no-debug            Turn on DEBUG logging  [default: no-debug]
  --execute TEXT                  Execute code string instead of sending
                                  kernel info request
  --install-completion [bash|zsh|fish|powershell|pwsh]
                                  Install completion for the specified shell.
  --show-completion [bash|zsh|fish|powershell|pwsh]
                                  Show completion for the specified shell, to
                                  copy it or customize the installation.
  --help                          Show this message and exit.
```

Try it out by starting an IPython kernel in one terminal and using the CLI in another.

```bash
python -m ipykernel_launcher --debug -f /tmp/kernel.json
```

```bash
kernel-sidecar on  release-0.3.2 [$?] is 📦 v0.3.1 via 🐍 v3.11.0 (kernel-sidecar-py3.11) 
❯ sidecar -f /tmp/kernel.json
2023-03-10T14:31:59.992235Z [info     ] Attempting to connect:
{'control_port': 34897,
 'hb_port': 49821,
 'iopub_port': 40577,
 'ip': '127.0.0.1',
 'kernel_name': '',
 'key': '615bcebc-baf2e28abad1f6c017dc71dc',
 'shell_port': 37421,
 'signature_scheme': 'hmac-sha256',
 'stdin_port': 41405,
 'transport': 'tcp'} [kernel_sidecar.cli] filename=cli.py func_name=main lineno=62
2023-03-10T14:32:00.026503Z [info     ] {'banner': 'Python 3.11.0 (main, Nov  7 2022, 09:38:45) [GCC 9.4.0]\n'
           "Type 'copyright', 'credits' or 'license' for more information\n"
           "IPython 8.10.0 -- An enhanced Interactive Python. Type '?' for "
           'help.\n',
 'debugger': None,
 'help_links': [{'text': 'Python Reference',
                 'url': 'https://docs.python.org/3.11'},
                {'text': 'IPython Reference',
                 'url': 'https://ipython.org/documentation.html'},
                {'text': 'NumPy Reference',
                 'url': 'https://docs.scipy.org/doc/numpy/reference/'},
                {'text': 'SciPy Reference',
                 'url': 'https://docs.scipy.org/doc/scipy/reference/'},
                {'text': 'Matplotlib Reference',
                 'url': 'https://matplotlib.org/contents.html'},
                {'text': 'SymPy Reference',
                 'url': 'http://docs.sympy.org/latest/index.html'},
                {'text': 'pandas Reference',
                 'url': 'https://pandas.pydata.org/pandas-docs/stable/'}],
 'implementation': 'ipython',
 'implementation_version': '8.10.0',
 'language_info': {'codemirror_mode': {'name': 'ipython', 'version': 3},
                   'file_extension': '.py',
                   'mimetype': 'text/x-python',
                   'name': 'python',
                   'nbconvert_exporter': 'python',
                   'pygments_lexer': 'ipython3',
                   'version': '3.11.0'},
 'protocol_version': '5.3',
 'status': 'ok'} [kernel_sidecar.cli] filename=cli.py func_name=connect lineno=44
```

```bash
❯ sidecar -f /tmp/kernel.json --execute "print('Hello, World'); 1/0"
2023-03-10T14:33:27.394935Z [info     ] Attempting to connect:
{'control_port': 34897,
 'hb_port': 49821,
 'iopub_port': 40577,
 'ip': '127.0.0.1',
 'kernel_name': '',
 'key': '615bcebc-baf2e28abad1f6c017dc71dc',
 'shell_port': 37421,
 'signature_scheme': 'hmac-sha256',
 'stdin_port': 41405,
 'transport': 'tcp'} [kernel_sidecar.cli] filename=cli.py func_name=main lineno=62
2023-03-10T14:33:27.629630Z [info     ] Hello, World
                  [kernel_sidecar.cli] filename=cli.py func_name=handle_stream lineno=23
2023-03-10T14:33:27.702700Z [error    ] division by zero               [kernel_sidecar.cli] filename=cli.py func_name=handle_error lineno=31
```
