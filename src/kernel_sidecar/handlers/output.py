import logging
from typing import List, Union

from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.comms import WidgetHandler
from kernel_sidecar.handlers.base import Handler
from kernel_sidecar.models import messages

logger = logging.getLogger(__name__)

ContentType = Union[
    messages.ExecuteResultContent,
    messages.StreamContent,
    messages.ErrorContent,
    messages.DisplayDataContent,
]


class OutputHandler(Handler):
    """
    Attach to Execute Requests to update a document model with the outputs coming back from the
    Kernel. This handler aims to handle Output widgets and display_data / update_displya_data
    messages which may update different parts of the document besides the currently running cell.
    """

    def __init__(self, client: KernelSidecarClient, cell_id: str):
        self.client = client
        self.cell_id = cell_id

        self.clear_on_next_output = False
        # .output_widget_contexts will be a list of Output widget Commhandler's. If user code enters
        # into its context ("with out1: print('foo')"), we should write to its state instead of
        # to the document model
        self.output_widget_contexts: List[WidgetHandler] = []

    # The following five methods should be overridden to update the document model using your own
    # Notebook builder implementation. The methods below that probably don't need to be overridden,
    # and take care of calling these five methods in the correct contexts (i.e. Output widget
    # context vs regular cell output, syncing Output widget content back to Kernel over comms,
    # clear_output(wait=True), and display data syncs)
    async def add_cell_content(self, content: ContentType):
        # Override in subclasses
        pass

    async def clear_cell_content(self):
        # Override in subclasses
        pass

    async def add_output_widget_content(self, handler: WidgetHandler, content: ContentType):
        # Override in subclasses
        pass

    async def clear_output_widget_content(self, handler: WidgetHandler):
        # Override in subclasses
        pass

    async def sync_display_data(
        self, content: Union[messages.DisplayDataContent, messages.UpdateDisplayDataContent]
    ):
        # Override in subclasses
        pass

    async def add_content(self, content: ContentType):
        if self.output_widget_contexts:  # inside a "with out:" Output widget context
            handler: WidgetHandler = self.output_widget_contexts[0]
            handler.state["outputs"].append(content)
            await self.sync_output_widget_state(handler)
            await self.add_output_widget_content(handler, content)
        else:  # not in Output widget context, just update Notebook document model
            await self.add_cell_content(content)

    async def clear_content(self):
        if self.output_widget_contexts:
            handler: WidgetHandler = self.output_widget_contexts[0]
            handler.state["outputs"] = []
            await self.sync_output_widget_state(handler)
            await self.clear_output_widget_content(handler)
        else:
            await self.clear_cell_content()

    async def sync_output_widget_state(self, handler: WidgetHandler):
        self.client.comm_msg_request(
            comm_id=handler.comm_id,
            data={"method": "update", "state": {"outputs": handler.state["outputs"]}},
        )

    async def handle_stream(self, msg: messages.Stream):
        if self.clear_on_next_output:
            await self.clear_content()
            self.clear_on_next_output = False
        await self.add_content(msg.content)

    async def handle_execute_result(self, msg: messages.ExecuteResult):
        if self.clear_on_next_output:
            await self.clear_content()
            self.clear_on_next_output = False
        await self.add_content(msg.content)

    async def handle_error(self, msg: messages.Error):
        if self.clear_on_next_output:
            await self.clear_content()
            self.clear_on_next_output = False
        await self.add_content(msg.content)

    async def handle_display_data(self, msg: messages.DisplayData):
        if self.clear_on_next_output:
            await self.clear_content()
            self.clear_on_next_output = False
        await self.add_content(msg.content)
        if msg.content.display_id:
            await self.sync_display_data(msg.content)

    async def handle_update_display_data(self, msg: messages.UpdateDisplayData):
        await self.sync_display_data(msg.content)

    async def handle_clear_output(self, msg: messages.ClearOutput):
        if msg.content.wait:
            self.clear_on_next_output = True
        else:
            await self.clear_content()

    async def handle_comm_msg(self, msg: messages.CommMsg):
        # Exit early if:
        #  - we don't recognize this comm id
        #  - It's not a comm for an Output Widget
        if msg.content.comm_id not in self.client.comm_manager.comms:
            return
        comm_handler = self.client.comm_manager.comms[msg.content.comm_id]
        if (
            not isinstance(comm_handler, WidgetHandler)
            or not comm_handler.model_name == "OutputModel"
        ):
            return
        if msg.content.data["method"] == "update":
            if msg.content.data["state"]["msg_id"]:
                # this means we are entering into the "with Output()" context manager
                # all further output messages (stream, display_data, error) should be added
                # to this Output widget .outputs instead of to the cell output
                logger.debug("entering output widget context manager")
                self.output_widget_contexts.insert(0, comm_handler)
            else:
                logger.debug("exiting output widget context manager")
                self.output_widget_contexts.remove(comm_handler)
