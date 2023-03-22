import asyncio
import json
import logging
import os

import pytest
from jupyter_client import AsyncKernelClient, manager
from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.log_utils import setup_logging

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope="session")
async def ipykernel() -> dict:
    """
    Starts a new ipykernel in a separate process. If you want to manually start an ipykernel
    to see Kernel debug logs or because it's set up as a separate step in CI jobs, set the
    IPYKERNEL_TEST_CONNECTION_FILE env variable to the .json connection file. For example:

    poetry shell
    python -m ipykernel_launcher --debug -f /tmp/kernel.json

    Then run pytest with:

    IPYKERNEL_TEST_CONNECTION_FILE=/tmp/kernel.json pytest
    """
    if "IPYKERNEL_TEST_CONNECTION_FILE" in os.environ:
        yield json.load(open(os.environ["IPYKERNEL_TEST_CONNECTION_FILE"]))
    else:
        km: manager.AsyncKernelManager
        kc: AsyncKernelClient
        km, kc = await manager.start_new_async_kernel()
        try:
            yield kc.get_connection_info()
        finally:
            logger.critical("Shutting down ipykernel")
            await km.shutdown_kernel()


@pytest.fixture
async def kernel(ipykernel: dict) -> KernelSidecarClient:
    async with KernelSidecarClient(connection_info=ipykernel) as kernel:
        yield kernel
        # reset namespace after test is done, turn off debug logs if they're on to reduce noise
        log_level = logging.getLogger("kernel_sidecar").getEffectiveLevel()
        if log_level == logging.DEBUG:
            logging.getLogger("kernel_sidecar").setLevel(logging.INFO)
        logger.critical("Using reset magic to clear Kernel state")
        action = kernel.execute_request(code="%reset -f in out dhist")
        await asyncio.wait_for(action, timeout=1)
        if log_level == logging.DEBUG:
            logging.getLogger("kernel_sidecar").setLevel(log_level)


@pytest.fixture(autouse=True)
def configure_logging():
    """
    Configure Structlog to log messages with `ConsoleRenderer` at the log level passed in to pytest:
     - pytest --log-level DEBUG -s
     - pytest --log-level INFO -s

    See log_utils.py for more comments and example of how you would set up structlog in your own app
    to process both structlog-emitted logs and vanilla logs (which kernel-sidecar emits)
    """
    setup_logging()
