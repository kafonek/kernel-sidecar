import asyncio
import json
import logging
import os

import pytest
from jupyter_client import AsyncKernelClient, manager
from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.log_utils import setup_logging
from kernel_sidecar.settings import get_settings

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope="session", autouse=True)
def configure_logging():
    """
    Configure Structlog to log messages with `ConsoleRenderer` at the log level passed in to pytest:
     - pytest --log-level DEBUG -s
     - pytest --log-level INFO -s

    See log_utils.py for more comments and example of how you would set up structlog in your own app
    to process both structlog-emitted logs and vanilla logs (which kernel-sidecar emits)
    """
    get_settings().pprint_logs = True
    setup_logging()


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
        logger.info(f"Using connection info from: {os.environ['IPYKERNEL_TEST_CONNECTION_FILE']}")
        yield json.load(open(os.environ["IPYKERNEL_TEST_CONNECTION_FILE"]))
    else:
        logger.info("Starting new AsyncKernel using jupyter_client")
        km: manager.AsyncKernelManager
        kc: AsyncKernelClient
        km, kc = await manager.start_new_async_kernel()
        try:
            yield kc.get_connection_info()
        finally:
            logger.info("Tests completed, shutting down ipykernel")
            try:
                await asyncio.wait_for(km.shutdown_kernel(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("Timed out waiting for kernel shutdown")


@pytest.fixture
async def kernel(ipykernel: dict) -> KernelSidecarClient:
    async with KernelSidecarClient(connection_info=ipykernel) as kernel:
        # Reset the Kernel namespace before passing the client to a test
        # The log level dance here is to reduce noise if DEBUG logs are on for the "shell reset"
        log_level = logging.getLogger("kernel_sidecar").getEffectiveLevel()
        if log_level == logging.DEBUG:
            logging.getLogger("kernel_sidecar").setLevel(logging.INFO)
        try:
            # like %reset magic but actually clears execution queue
            # see https://github.com/ipython/ipython/issues/13087
            action = kernel.execute_request(
                code="get_ipython().kernel.shell.reset(new_session=True, aggressive=True)",
                silent=True,
            )
            # This is set to 15 because after hours of debugging CI, it seems like sometimes the
            # jupyter_manager HB client watcher can take ~10 seconds awaiting a connection, and
            # bumping this to 15 seconds makes the tests less flaky
            await asyncio.wait_for(action, timeout=15)
            kernel.actions.pop(action.request.header.msg_id)
        except asyncio.TimeoutError:
            logger.warning("Timed out waiting to reset Kernel state")
        if log_level == logging.DEBUG:
            logging.getLogger("kernel_sidecar").setLevel(log_level)

        kernel._handler_timeout = 5  # set a short timeout for tests to
        yield kernel
