import logging

from malevich.core_api import set_logger

logger = logging.getLogger('malevich.core')
handles = [*logger.handlers]
set_logger(logger)

class IgnoreCoreLogs:
    def __enter__(self, *args):
        logger.setLevel(logging.CRITICAL + 1)
        logger.handlers = []
        logger.propagate = False
        set_logger(logger)

        return self

    def __exit__(self, *args) -> None:
        logger.setLevel(logging.NOTSET)
        logger.handlers = handles
        logger.propagate = True
        set_logger(logger)

