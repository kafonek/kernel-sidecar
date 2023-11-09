import datetime

import pydantic
from dateutil.tz import tzlocal

from kernel_sidecar.models.messages import Message


async def test_kernel_info_missing_codemirror_mode():
    """
    This test added after observing a bug when parsing kernel_info_reply from Deno kernel which
    does not set the codemirror_mode (spec says if that is empty to default to language_info name)
    """
    msg = {
        "buffers": [],
        "content": {
            "banner": "Welcome to Deno kernel",
            "help_links": [{"text": "Visit Deno manual", "url": "https://deno.land/manual"}],
            "implementation": "Deno kernel",
            "implementation_version": "1.37.0",
            "language_info": {
                "file_extension": ".ts",
                "mimetype": "text/x.typescript",
                "name": "typescript",
                "nb_converter": "script",
                "pygments_lexer": "typescript",
                "version": "5.2.2",
            },
            "protocol_version": "5.3",
            "status": "ok",
        },
        "header": {
            "date": "2023-09-21T15:27:15.660108339+00:00",
            "msg_id": "a53857b2-5f17-4d73-ac98-68990adb652b",
            "msg_type": "kernel_info_reply",
            "session": "0133bc47-8929-4edb-8d16-0bcaa63c5b9e",
            "username": "kernel",
            "version": "5.3",
        },
        "metadata": {},
        "msg_id": "a53857b2-5f17-4d73-ac98-68990adb652b",
        "msg_type": "kernel_info_reply",
        "parent_header": {
            "date": datetime.datetime(2023, 9, 21, 15, 27, 15, 659657, tzinfo=tzlocal()),
            "msg_id": "49fd7d2c-01d7-41ee-ad27-6580f7871a49",
            "msg_type": "kernel_info_request",
            "session": "0133bc47-8929-4edb-8d16-0bcaa63c5b9e",
            "username": "kernel-sidecar",
            "version": "5.3",
        },
    }
    message = pydantic.TypeAdapter(Message).validate_python(msg)
    assert message.content.language_info.codemirror_mode == "typescript"
