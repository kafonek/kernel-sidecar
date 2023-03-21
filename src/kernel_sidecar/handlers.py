import collections
import logging

from kernel_sidecar.models import messages
from kernel_sidecar.nb_builder import NotebookBuilder

logger = logging.getLogger(__name__)


class Handler:
    """
    Base class for delegating messages to methods defined in subclasses by msg_type. Use:

    class StatusHandler(Handler):
        async def handle_status(self, msg: messages.Status):
            print(f"Kernel status: {msg.content.execution_state}")

    action = kernel.kernel_info_request(handlers=[StatusHandler()])
    await action

    >>> Kernel status: busy
    >>> Kernel status: idle
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


class OutputHandler(Handler):
    """
    Attach to Execute Requests to update a document model with the outputs coming back from the
    Kernel. This handler aims to handle Output widgets and display_data / update_displya_data
    messages which may update different parts of the document besides the currently running cell.
    """

    def __init__(self, cell_id: str, builder: NotebookBuilder):
        self.cell_id = cell_id
        self.builder = builder
        self.clear_on_next_output = False

    async def handle_stream(self, msg: messages.Stream):
        if self.clear_on_next_output:
            self.builder.clear_output(self.cell_id)
            self.clear_on_next_output = False
        self.builder.add_output(self.cell_id, msg.content)

    async def handle_execute_result(self, msg: messages.ExecuteResult):
        if self.clear_on_next_output:
            self.builder.clear_output(self.cell_id)
            self.clear_on_next_output = False
        self.builder.add_output(self.cell_id, msg.content)

    async def handle_error(self, msg: messages.Error):
        if self.clear_on_next_output:
            self.builder.clear_output(self.cell_id)
            self.clear_on_next_output = False
        self.builder.add_output(self.cell_id, msg.content)

    async def handle_display_data(self, msg: messages.DisplayData):
        if self.clear_on_next_output:
            self.builder.clear_output(self.cell_id)
            self.clear_on_next_output = False
        self.builder.add_output(self.cell_id, msg.content)
        if msg.content.display_id:
            self.builder.replace_display_data(msg.content)

    async def handle_update_display_data(self, msg: messages.UpdateDisplayData):
        self.builder.replace_display_data(msg.content)

    async def handle_clear_output(self, msg: messages.ClearOutput):
        if msg.content.wait:
            self.clear_on_next_output = True
        else:
            self.builder.clear_output(self.cell_id)
