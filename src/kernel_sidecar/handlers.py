import collections

from kernel_sidecar.models import messages


class Handler:
    """
    Base class for delegating messages to methods defined in subclasses by msg_type. Use:

    class StatusHandler(Handler):
        async def handle_status(self, msg: messages.Status):
            print(f"Kernel status: {msg.content.execution_state}")

    action = kernel.kernel_info_request(handlers=[StatusHandler()])
    await action
    """

    async def __call__(self, msg: messages.Message):
        handler = getattr(self, f"handle_{msg.msg_type}", None)
        if handler:
            await handler(msg)
        else:
            await self.unhandled_message(msg)

    async def unhandled_message(self, msg: messages.Message):
        """
        Called when a message is delegated to this Action but no handler is defined for the msg_type
        """
        pass


class DebugHandler(Handler):
    """
    Useful for testing and debugging. Example:

    handler = DebugHandler()
    action = kernel.kernel_info_request(handlers=[handler])
    await action
    assert handler.counts == {"status": 2, "kernel_info_reply": 1}
    """

    def __init__(self):
        self.counts = collections.defaultdict(int)
        self.last_msg_by_type = collections.defaultdict(messages.Message)

    async def unhandled_message(self, msg: messages.Message):
        # effectively a catch-all for every message type since no other handlers are defined
        self.counts[msg.msg_type] += 1
        self.last_msg_by_type[msg.msg_type] = msg
