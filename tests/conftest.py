import logging

import pytest
import structlog
from jupyter_client import AsyncKernelClient, manager
from kernel_sidecar import Kernel


@pytest.fixture
async def ipykernel() -> dict:
    """
    Starts a new ipykernel in a separate process per test.

    Developers: if you need to see debug logs from the kernel, start an ipykernel manually
    with `python -m ipykernel_launcher --debug` and look for the connection file  (probably
    something like: ~/.local/share/jupyter/runtime/kernel-123.json). Instead of yielding
    `kc.get_connection_info()` below, `yield json.load(open(path_to_connection_file)`
    """
    km: manager.AsyncKernelManager
    kc: AsyncKernelClient
    km, kc = await manager.start_new_async_kernel()
    try:
        yield kc.get_connection_info()
    finally:
        await km.shutdown_kernel()


@pytest.fixture
async def kernel(ipykernel: dict) -> Kernel:
    async with Kernel(connection_info=ipykernel) as kernel:
        yield kernel


@pytest.fixture(autouse=True)
def configure_logging():
    """
    This fixture lets us control the logging from pytest args,
     - pytest --log-level DEBUG -s
     - pytest --log-level INFO -s
    """
    structlog.stdlib.recreate_defaults(log_level=logging.getLogger().level)
