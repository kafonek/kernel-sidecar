# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed
- Try to guard against deadlocked / awaiting-forever Actions by checking if incoming messages have a parent header msg_id for a different Action than what we think is running
- Try to harden CI test runs, many of which were getting stuck during fixture teardowns at the end of the test runs
- 
### Fixed
- Revert some critical logs to debug, accidentally left over from Output widget handler development

## [0.4.0] - 2023-03-22
### Added
- `OutputHandler` and a `NotebookBuilder` to handle receiving replies from an execute request and updating outputs in a Notebook model
  - Supports updating `display_data` content when new `display_data` with `transient` / `display_id` comes in or on `update_display_data`
  - Supports writing state to `Output` widgets, sending `comm_msg` back to Kernel to update that, and rehydrating a document model with Output widget state

### Fixed
- Was missing `clear_output` model

## [0.3.2] -- 2023-03-10
### Added
- CLI, `sidecar --help`
  - `sidecar -f <connection-file>` to send a kernel info request, see if the connection works
  - `sidecar -f <connection-file> --execute <code>` to send an execute request
  - `sidecar -f <connection-file> --debug` to log out DEBUG (all ZMQ messages)

### Changed
- Relaxed some of the Pydantic models after trying this out with Rust kernel, which doesn't quite fit Jupyter message spec
- DEBUG logs using `pprint.pformat`, and some noisier logging around validation errors when parsing ZMQ messages

## [0.3.1] - 2023-03-02

### Added
- Default handlers that can be attached to every Action created from `kernel.send`
- Comm Manager that is attached to every Action in `kernel.send`
  
## [0.3.0] - 2023-02-24

### Changed
- Renamed `kernel_sidecar.kernel.SidecarKernelClient` to `kernel_sidecar.client.KernelSidecarClient` for better accuracy / consistency

## [0.2.1] - 2023-02-24

### Added
- `SidecarKernelClient.running_action` property to return what action the Kernel is probably handling. Useful for testing
- Callback hook in the `SidecarKernelClient` to take action on ZMQ channel disconnect
- Moved `DebugHandler` out of tests and into `handlers.py` so it can be used for testing by downstream apps

## [0.2.0] - 2023-02-22

### Changed
- Significant refactors after working on integrating `kernel-sidecar` into a production app
  - `Kernel` renamed to `SidecarKernelClient`
  - `request` models (`execute_request`, `kernel_info_request`, etc) are split out into their own module from `actions`
  - `actions` are now primarily responsible for knowing when they're complete (`awaitable`) and attaching 0-n `handlers`
  - `action.observe` removed since you can attach more than one `handler`. It was unclear when to use `observers` vs `handlers`

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