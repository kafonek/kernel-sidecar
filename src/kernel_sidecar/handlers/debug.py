import collections

from kernel_sidecar.handlers.base import Handler
from kernel_sidecar.models import messages


class DebugHandler(Handler):
    """
    Useful for testing and debugging. Example:

    handler = DebugHandler()
    action = kernel.kernel_info_request(handlers=[handler])
    await action
    assert handler.counts == {"status": 2, "kernel_info_reply": 1}

    kernel_info_reply: messages.KernelInfoReply = handler.get_last_msg("kernel_info_reply")
    assert kernel_info_reply.status == "ok"
    """

    def __init__(self):
        self.counts = collections.defaultdict(int)
        # don't access this in tests like "last_msg = handler.last_msg_by_type['status']" becuse
        # it will raise an obtuse error saying a typing.Union cannot be called. What's happening
        # is that if the key is missing, defaultdict tries to instantiate a messages.Message which
        # is a typing.Annotated[typing.Union]] (discriminator pattern) and everything blows up.
        # use .get_last_msg() instead.
        self.last_msg_by_type = collections.defaultdict(messages.Message)

    def get_last_msg(self, msg_type: str) -> messages.Message:
        if msg_type not in self.last_msg_by_type:
            raise KeyError(f"No message of type {msg_type} has been received")
        return self.last_msg_by_type[msg_type]

    async def unhandled_message(self, msg: messages.Message):
        # effectively a catch-all for every message type since no other handlers are defined
        self.counts[msg.msg_type] += 1
        self.last_msg_by_type[msg.msg_type] = msg
