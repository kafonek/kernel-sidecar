"""
Actions encompass the request-reply cycle for a request to the Kernel and all the messages that
the Kernel will emit based on that request. Importantly, an Action is awaitable, so that you can
run until a cycle is "complete", which usually means seeing the Kernel transition status back to
idle and for it to emit a specific type of reply based on the request.

Business logic, such as updating a document model state or emitting messages back to a central
server / frontends, should be implemented in handlers that are attached to each Action.
"""
import asyncio
import logging
from typing import List

from kernel_sidecar.handlers import Handler
from kernel_sidecar.models import messages, requests

logger = logging.getLogger(__name__)

REPLY_MSG_TYPES = {
    "kernel_info_request": "kernel_info_reply",
    "execute_request": "execute_reply",
    "inspect_request": "inspect_reply",
    "complete_request": "complete_reply",
    "history_request": "history_reply",
    "is_complete_request": "is_complete_reply",
    "comm_info_request": "comm_info_reply",
    "shutdown_request": "shutdown_reply",
    "interrupt_request": "interrupt_reply",
    "debug_request": "debug_reply",
    "comm_open": None,
    "comm_msg": None,
    "comm_close": None,
}


class KernelAction:
    REPLY_MSG_TYPES = REPLY_MSG_TYPES

    def __init__(self, request: requests.Request, handlers: List[Handler] = None):
        self.request = request
        if request.header.msg_type not in self.REPLY_MSG_TYPES:
            raise ValueError(
                f"Unrecognized request type {request.header.msg_type}. Raising error because "
                "the KernelAction would not know when the request-reply cycle is finished. If you "
                "have a custom request type, add it to KernelAction.REPLY_MSG_TYPES."
            )
        self.expected_reply_msg_type = self.REPLY_MSG_TYPES[request.header.msg_type]

        # Events tied to making this instance awaitable
        self.kernel_idle = asyncio.Event()
        self.reply_seen = asyncio.Event()
        self.done = asyncio.Event()

        # Routing messages to handlers
        self.handlers = handlers or []

        # Flips to True during kernel.send, and kernel.send won't send anything over ZMQ if the
        # Action.sent is True. Avoid weird edge cases with routing messages to different Action
        # instances that might have same request msg_id
        self.sent = False

    @property
    def msg_id(self):
        return self.request.header.msg_id

    @property
    def msg_type(self):
        return self.request.header.msg_type

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.msg_type} {self.msg_id}>"

    def __await__(self) -> asyncio.Event:
        """Support 'await action' syntax"""
        return self.done.wait().__await__()

    def maybe_set_done(self):
        """
        Set the Action as "done", meaning we've seen all expected replies from the Kernel
         - always wait until Kernel has reported being Idle with our parent header msg id
         - Depending on the request type, also wait for an expected reply type, e.g.
           execute_request isn't "complete" until we get execute_reply
        """
        if self.kernel_idle.is_set():
            if self.reply_seen.is_set() or not self.expected_reply_msg_type:
                self.done.set()

    async def handle_message(self, msg: messages.Message):
        """Delegate message to the appropriate handler defined in subclasses"""
        # Checking for status / special reply type in order to maybe set "done"
        if msg.msg_type == "status":
            if msg.content.execution_state == "idle":
                self.kernel_idle.set()
                self.maybe_set_done()
        elif msg.msg_type == self.expected_reply_msg_type:
            self.reply_seen.set()
            self.maybe_set_done()

        # Delegate the message to any attached handlers, in the order they were attached
        for handler in self.handlers:
            await handler(msg)
