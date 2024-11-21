import logging
from enum import Enum

from rich.logging import RichHandler

from malevich.manifest import manf

from ..models.actions import Action
from ..models.preferences import LogFormat, UserPreferences, VerbosityLevel

prefs = manf.query('preferences')
prefs = UserPreferences(**prefs) if prefs else UserPreferences()
logger = logging.getLogger('malevich')

if not any([x.name == 'META_H' for x in logger.handlers]):
    for h in logger.handlers:
        logger.removeHandler(h)

    logger.setLevel(logging.DEBUG)
    handler = (
        logging.StreamHandler()
        if prefs.log_format == LogFormat.Plain
        else RichHandler(markup=True)
    )
    handler.setLevel(logging.DEBUG)
    handler.set_name("META_H")
    formatter = logging.Formatter("[b](%(name)s)[/b] %(message)s", datefmt="[%X]")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


class LogLevel(Enum):
    Info = "INFO"
    Warning = "WARNING"
    Error = "ERROR"
    Debug = "DEBUG"


def _cout(
    message: str,
    level: LogLevel = LogLevel.Info,
    *args
) -> None:
    if level == LogLevel.Info:
        logger.info(message, extra={"markup": True})
    elif level == LogLevel.Warning:
        logger.warning(message, extra={"markup": True})
    elif level == LogLevel.Error:
        logger.error(message, extra={"markup": True})
    elif level == LogLevel.Debug:
        logger.debug(message, extra={"markup": True})


def cout(
    action: Action,
    message: str,
    verbosity: VerbosityLevel = VerbosityLevel.Quiet,
    level: LogLevel = LogLevel.Info,
    *args,
) -> None:
    prefs: UserPreferences = manf.query('preferences')
    if not prefs:
        prefs = UserPreferences()
    else:
        prefs = UserPreferences(**prefs)
    if prefs.verbosity[action.value] == VerbosityLevel.Quiet:
        return
    elif verbosity.value <= prefs.verbosity[action.value]:
        if prefs.log_format == LogFormat.Plain:
            _cout(f"({action.name}) {message}", level=level, *args)
        else:
            _cout(f"[medium_purple2][{action.name}][/medium_purple2] {message}",
                  level=level,
                  *args
            )
    else:
        return
