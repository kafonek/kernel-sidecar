import collections
import textwrap
from typing import Optional

from jupyter_client import KernelConnectionInfo
from kernel_sidecar.handlers import DebugHandler
from kernel_sidecar.kernel import SidecarKernelClient


class DisconnectHandlingClient(SidecarKernelClient):
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
        # Setting the two event states here will make the "await action" resolve when either status
        # or reply event is seen next.
        # Not a perfect solution, tune this behavior in your own app.
        self.running_action.kernel_idle.set()
        self.running_action.reply_seen.set()
        self.channel_disconnects[channel_name] += 1


async def test_zmq_disconnect(ipykernel: dict):
    """
    Show that
    """
    async with DisconnectHandlingClient(connection_info=ipykernel, max_message_size=1024) as kernel:
        handler = DebugHandler()
        code = textwrap.dedent(
            """
        print('x' * 2048)
        """
        )
        action = kernel.execute_request(code=code, handlers=[handler])
        await action
        # TODO: watch for this test getting flaky? On local dev, it always misses the second status
        # message during reconnect
        assert handler.counts == {"status": 1, "execute_input": 1, "execute_reply": 1}
        assert kernel.channel_disconnects == {"iopub": 1}
