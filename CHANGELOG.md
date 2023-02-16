# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.1.5] - 2023-02-16

### Added
- Model `input_request`
- Add `.send_stdin` method on `Kernel`
- `Kernel` watches `stdin` channel

## [0.1.4] - 2023-02-15

### Changed
- `actions.KernelActionBase.request_header` updated to a custom model so you can easily override e.g. `action.request_header.username = "cell1"` if you wanted to track cell id's in zmq messages
- `kernel.request` and convenience functions that call into that (`kernel.execute_request`, `kernel.kernel_info_request`, etc) switched to be sync instead of async methods

## [0.1.3] - 2023-02-10

### New
- Add badges in README, build [empty] docs

### Changed
- Drop `jupyter_client` dependency down

## [0.1.2] - 2023-02-10

### New
- Initial release
 - `Kernel` to manage zmq connection to external kernel process
 - `messages` to model ZMQ messages as Pydantic models
 - `notebook` to model a nbformat Notebook as Pydantic models
 - `actions` to encapsulate a request-reply flow between sidecar and kernel