import logging
from typing import List

from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.comms import WidgetHandler
from kernel_sidecar.handlers.base import Handler
from kernel_sidecar.models import messages
from kernel_sidecar.nb_builder import ContentType

logger = logging.getLogger(__name__)


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

    async def add_output(self, content: ContentType):
        if self.output_widget_contexts:  # in Output context, don't update document model
            handler: WidgetHandler = self.output_widget_contexts[0]
            handler.state["outputs"].append(content)
            await self.sync_output_widget_state(handler)
        else:  # not in Output widget context, just update Notebook document model
            self.client.builder.add_output(self.cell_id, content)

    async def clear_output(self):
        if self.output_widget_contexts:
            handler: WidgetHandler = self.output_widget_contexts[0]
            handler.state["outputs"] = []
            await self.sync_output_widget_state(handler)
        else:
            self.client.builder.clear_output(self.cell_id)

    async def sync_output_widget_state(self, handler: WidgetHandler):
        self.client.comm_msg_request(
            comm_id=handler.comm_id,
            data={"method": "update", "state": {"outputs": handler.state["outputs"]}},
        )

    async def handle_stream(self, msg: messages.Stream):
        if self.clear_on_next_output:
            await self.clear_output()
            self.clear_on_next_output = False
        await self.add_output(msg.content)

    async def handle_execute_result(self, msg: messages.ExecuteResult):
        if self.clear_on_next_output:
            await self.clear_output()
            self.clear_on_next_output = False
        await self.add_output(msg.content)

    async def handle_error(self, msg: messages.Error):
        if self.clear_on_next_output:
            await self.clear_output()
            self.clear_on_next_output = False
        await self.add_output(msg.content)

    async def handle_display_data(self, msg: messages.DisplayData):
        if self.clear_on_next_output:
            await self.clear_output()
            self.clear_on_next_output = False
        await self.add_output(msg.content)
        if msg.content.display_id:
            self.client.builder.replace_display_data(msg.content)

    async def handle_update_display_data(self, msg: messages.UpdateDisplayData):
        self.client.builder.replace_display_data(msg.content)

    async def handle_clear_output(self, msg: messages.ClearOutput):
        if msg.content.wait:
            self.clear_on_next_output = True
        else:
            await self.clear_output()

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
