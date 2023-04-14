import asyncio
import json
import logging
import pathlib
import pprint
from typing import Optional

import typer
from jupyter_client import KernelConnectionInfo

from kernel_sidecar.client import KernelSidecarClient
from kernel_sidecar.handlers.base import Handler
from kernel_sidecar.handlers.debug import DebugHandler
from kernel_sidecar.log_utils import setup_logging
from kernel_sidecar.models import messages
from kernel_sidecar.settings import get_settings

logger = logging.getLogger(__name__)

app = typer.Typer(no_args_is_help=True)


class OutputHandler(Handler):
    async def handle_stream(self, msg: messages.Stream):
        if msg.content.name == "stdout":
            logger.info(msg.content.text)
        elif msg.content.name == "stderr":
            logger.error(msg.content.text)

    async def handle_execute_result(self, msg: messages.ExecuteResult):
        logger.info(pprint.pformat(msg.content.data))

    async def handle_error(self, msg: messages.Error):
        logger.error(msg.content.evalue)


async def execute_code(connection_info: KernelConnectionInfo, code: str):
    async with KernelSidecarClient(connection_info) as kernel:
        await kernel.execute_request(code=code, handlers=[OutputHandler()])


async def connect(connection_info: KernelConnectionInfo, tail: bool):
    async with KernelSidecarClient(connection_info) as kernel:
        handler = DebugHandler()
        await kernel.kernel_info_request(handlers=[handler])
        kernel_info: messages.KernelInfoReply = handler.get_last_msg("kernel_info_reply")
        logger.info(pprint.pformat(kernel_info.content.dict()))
        if tail:
            while True:
                await asyncio.sleep(0.01)


@app.command()
def main(
    connection_file: pathlib.Path = typer.Option(
        ..., "-f", help="Kernel connection file", exists=True, dir_okay=False
    ),
    debug: bool = typer.Option(default=False, help="Turn on DEBUG logging"),
    execute: Optional[str] = typer.Option(
        default=None, help="Execute code string instead of sending kernel info request"
    ),
    tail: Optional[bool] = typer.Option(
        default=False, help="Continue tailing ZMQ after connecting or executing code"
    ),
):
    if debug:
        get_settings().pprint_logs = True
        setup_logging(log_level=logging.DEBUG)
    else:
        setup_logging()
    connection_info = json.loads(connection_file.read_text())
    logger.info(f"Attempting to connect:\n{pprint.pformat(connection_info)}")
    if execute:
        asyncio.run(execute_code(connection_info, execute))
    else:
        asyncio.run(connect(connection_info, tail=tail))


if __name__ == "__main__":
    app()
