import asyncio
import collections
import logging
from typing import Optional

from jupyter_client import KernelConnectionInfo
from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.handlers.debug import DebugHandler

logger = logging.getLogger(__name__)


class DisconnectHandlingClient(KernelSidecarClient):
    def __init__(
        self,
        connection_info: KernelConnectionInfo,
        max_message_size: Optional[int] = None,
    ):
        super().__init__(connection_info, max_message_size=max_message_size)
        self.channel_disconnects = collections.defaultdict(int)

    async def handle_zmq_disconnect(self, channel_name: str):
        # We might miss messages like execute_reply or status while reconnecting, which would cause
        # the currently-running action to never resolve it's "done" event (await action).
        # For this test class, "resolve" the awaitable as soon as a disconnect happens.
        # You might want to do something different in your own prod app
        logger.info(f"ZMQ channel {channel_name} disconnected")
        if self.running_action:
            logger.info(f"Resolving running action {self.running_action}")
            self.running_action.done.set()
            self.running_action.running = False
        self.channel_disconnects[channel_name] += 1


async def test_zmq_disconnect(ipykernel: dict):
    """
    Show that a subclassed Kernel with zmq handling hooks takes action when zmq iopub channel
    gets disconnected because a message is larger than the max_message_size setting.
    """
    kernel: DisconnectHandlingClient  # type hint here, can't hint while entering context manager
    async with DisconnectHandlingClient(connection_info=ipykernel, max_message_size=1024) as kernel:
        # A small stream output should come through fine
        handler = DebugHandler()
        action = kernel.execute_request(code="print('x')", handlers=[handler])
        await action
        assert handler.counts == {"status": 2, "execute_input": 1, "execute_reply": 1, "stream": 1}

        # A stream size larger than max_message_size should cause a disconnect
        handler = DebugHandler()
        action = kernel.execute_request(code="print('x' * 2048)", handlers=[handler])
        await asyncio.wait_for(action, timeout=3)
        assert kernel.channel_disconnects == {"iopub": 1}
