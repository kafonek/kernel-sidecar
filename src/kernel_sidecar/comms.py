"""
Comms are a flexible way for the Kernel and clients (historically frontends in our case the sidecar)
to communicate, particularly when the Kernel wants to trigger some action on the frontend. The
ipywidgets library is probably the most well known use of Comms in Jupyter. When a widget is created
(via execute_request), it opens three comms. If something on the kernel side changes the layout,
style, or value of the widget, the Kernel can emit comm_msg for those comm_id's to tell frontend
to update how it renders the widgets. If the user interacts with the widget, the frontend emits
comm_msg's to the Kernel to tell it to update the widget state.

Within kernel-sidecar, Comms are a bit tricky because they don't fit in with the other "Action"
request-reply handler paradigm. An execute_request might cause comm_msg's to be emitted, or sending
a comm_msg request might get comm_msg's in the replies. The way we handle this is to attach a single
CommManager Handler to every Action that is sent. CommManager will handle comm_* messages seen in
any request type. It can then delegate comm_msg's to appropriate CommHandler instances based on the
comm_id seen in the comm_msg.content.
"""
import logging
from typing import Dict, Type, Union

from kernel_sidecar.handlers import Handler
from kernel_sidecar.models import messages

logger = logging.getLogger(__name__)


class CommHandler(Handler):
    def __init__(self, comm_id: str):
        self.comm_id = comm_id

    def __repr__(self):
        return f"{self.__class__.__name__} ({self.comm_id})"


class CommManager(Handler):
    def __init__(self, handlers: Dict[str, Type[CommHandler]] = None):
        self.comms: Dict[str, CommHandler] = {}  # keys are comm_id, values are CommHandler instance
        self.handlers = handlers or {}  # keys are target_name, values are CommHandler class

    async def handle_comm_open(self, msg: messages.CommOpen):
        """
        Every comm_open has a comm_id, target_name, and optional data. From the managers perspective
        we need to map {comm_id: handler} based on a lookup of {target_name: handler} so that when
        we see comm_msg in the future, which only guarentee comm_id not target_name, we can route
        messages to the appropriate handler
        """
        comm_id = msg.content.comm_id
        target_name = msg.content.target_name
        if comm_id in self.comms:
            # Seems like a weird situation if we see a second comm open for the same comm_id?
            # Don't think it's technically against the spec.
            handler = self.comms[comm_id]
        else:
            if target_name not in self.handlers:
                return await self.handle_unrecognized_comm_target(msg)
            handler_cls = self.handlers[target_name]
            handler = handler_cls(comm_id=comm_id)
            self.comms[comm_id] = handler
            logger.debug("registered comm", extra={"comm_id": comm_id, "handler": handler})
        await handler(msg)

    async def handle_comm_msg(self, msg: messages.CommMsg):
        comm_id = msg.content.comm_id
        if comm_id not in self.comms:
            return await self.handle_unrecognized_comm_id(msg)
        handler = self.comms[comm_id]
        await handler(msg)

    async def handle_comm_close(self, msg: messages.CommClose):
        comm_id = msg.content.comm_id
        if comm_id not in self.comms:
            return await self.handle_unrecognized_comm_id(msg)
        handler = self.comms[comm_id]
        await handler(msg)
        del self.comms[comm_id]

    async def handle_unrecognized_comm_target(self, msg: messages.CommOpen):
        """
        Drop into here when we observe a comm_open with a target_name that we don't have mapped to
        a handler class. If you have a "catch-all" handler, this is a good place to set that up.

        E.g.
        self.handlers[msg.content.target_name] = MyCatchAllHandler
        return await self.handle_comm_open(msg)
        """
        pass

    async def handle_unrecognized_comm_id(self, msg: Union[messages.CommMsg, messages.CommClose]):
        """
        If we're seeing comm_msg or comm_close with a comm_id we aren't storing, then it's likely
        either another client is talking to the kernel (bad) or there was a comm_open earlier with
        a target_name we didn't have a handler mapped for, so now we have comm_msg's we're just not
        handling.
        """
        logger.debug("unrecognized comm_id", extra={"comm_id": msg.content.comm_id})
        pass


class CommTargetNotFound(Exception):
    pass


class CommOpenHandler(Handler):
    """
    Used when sending a comm_open request from sidecar to kernel. If there is no Comm registered
    for the target_name, the Kernel will say as much in a stream (stderr) reply, and send a
    comm_close event.
    """

    def __init__(self):
        self.comm_err_msg = None
        self.comm_closed_id = None

    async def handle_stream(self, msg: messages.Stream):
        if msg.content.name == "stderr":
            self.comm_err_msg = msg.content.text

    async def handle_comm_close(self, msg: messages.CommClose):
        self.comm_closed_id = msg.content.comm_id
