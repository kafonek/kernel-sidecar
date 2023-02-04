import logging

import pytest
import structlog
from jupyter_client import AsyncKernelClient, manager
from kernel_sidecar import Kernel


@pytest.fixture
async def ipykernel() -> dict:
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
    structlog.stdlib.recreate_defaults(log_level=logging.getLogger().level)
