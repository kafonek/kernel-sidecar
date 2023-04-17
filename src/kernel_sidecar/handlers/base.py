from kernel_sidecar.models import messages


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

    async def action_complete(self):
        """
        Called when the KernelAction this Handler is attached to is complete
        """
        pass
