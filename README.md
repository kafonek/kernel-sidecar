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

## Models

`kernel-sidecar` has Pydantic models for:
 - The Jupyter Notebook document (`models/notebook.py`), which should be consistent with `nbformat` parsing / structure
 - Request messages sent to the Kernel over ZMQ (`models/requests.py`)
 - Messages received over ZMQ from the Kernel (`models/messages.py`)



