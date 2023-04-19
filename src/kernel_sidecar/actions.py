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
from typing import List, Optional

from kernel_sidecar.handlers.base import Handler
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

    def __init__(self, request: requests.Request, handlers: Optional[List[Handler]] = None):
        self.request = request
        if request.header.msg_type not in self.REPLY_MSG_TYPES:
            raise ValueError(
                f"Unrecognized request type {request.header.msg_type}. Raising error because "
                "the KernelAction would not know when the request-reply cycle is finished. If you "
                "have a custom request type, add it to KernelAction.REPLY_MSG_TYPES."
            )
        self.expected_reply_msg_type = self.REPLY_MSG_TYPES[request.header.msg_type]

        # Events tied to making this instance awaitable
        self.running = False
        self.kernel_idle = asyncio.Event()  # gets set when kernel status reports idle
        self.reply_seen = asyncio.Event()  # gets set when we see execute_reply or the like
        self.done = asyncio.Event()
        self.safety_net_task = None  # see .kernel_idle_safety_net docstring

        # Routing messages to handlers
        self.handlers = handlers or []

        # Flips to True during kernel.send, and kernel.send won't send anything over ZMQ if the
        # Action.sent is True. Avoids weird edge cases with routing messages to different Action
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

    async def maybe_set_done(self):
        """
        Set the Action as "done", meaning we've seen all expected replies from the Kernel
         - always wait until Kernel has reported being Idle with our parent header msg id
         - Depending on the request type, also wait for an expected reply type, e.g.
           execute_request isn't "complete" until we get execute_reply
        """
        if self.kernel_idle.is_set():
            if self.reply_seen.is_set() or not self.expected_reply_msg_type:
                for handler in self.handlers:
                    await handler.action_complete()

                self.running = False
                self.done.set()
                if self.safety_net_task:
                    self.safety_net_task.cancel()

    async def kernel_idle_safety_net(self):
        """
        Sometimes we just don't see the expected reply, who knows why. It seems most common with
        execute_reply, especially during tests running in CI but I've seen it happen in prod too.
        """
        await asyncio.sleep(3)  # Todo: decide if this needs to be a setting
        if self.running and self.expected_reply_msg_type:
            logger.warning(
                f"Action {self} still running 3 seconds after Kernel went idle. Expected to see "
                f"{self.expected_reply_msg_type} by now but have not. Setting done anyway."
            )
            self.reply_seen.set()
            await self.maybe_set_done()

    async def handle_message(self, msg: messages.Message):
        """
        Delegate message to the appropriate handler defined in subclasses and try to determine
        if this Action is "done", meaning the Kernel has cycled from busy to idle and we've seen
        an expected message reply type (or kicked off a "safety net" task since sometimes we do
        not see the expected reply, especially for execute_request / execute_reply)
        """
        # Delegate the message to any attached handlers, in the order they were attached
        # TODO: consider pros and cons of asyncio.gather handlers instead of awaiting serially
        for handler in self.handlers:
            await handler(msg)

        # Checking for status / special reply type in order to maybe set "done"
        if msg.msg_type == "status":
            if msg.content.execution_state == "busy":
                self.running = True
            elif msg.content.execution_state == "idle":
                self.kernel_idle.set()
                await self.maybe_set_done()
                # Normally shouldn't see kernel go idle before we see the expected reply type
                # but hence the name, this is a safety net
                if self.running:
                    logger.debug(f"Creating safety net task for {self}")
                    self.safety_net_task = asyncio.create_task(self.kernel_idle_safety_net())

        elif msg.msg_type == self.expected_reply_msg_type:
            self.reply_seen.set()
            await self.maybe_set_done()
