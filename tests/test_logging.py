def test_log_level():
    import logging

    import structlog

    print(logging.getLogger())
    print(logging.getLogger().level)
    structlog.stdlib.recreate_defaults(log_level=logging.getLogger().level)

    logger = structlog.get_logger(__name__)

    logger.debug("debug")
    logger.info("info")
    logger.warning("warning")
    logger.error("error")
    logger.error("error")
