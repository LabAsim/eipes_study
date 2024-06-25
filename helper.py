import copy
import logging
import os
import pathlib

import colorama

logger = logging.getLogger(__name__)


class LoggingFormatter(logging.Formatter):
    """A custom Formatter with colors for each logging level"""

    format = "%(levelname)s: %(name)s |  %(message)s"
    #
    FORMATS = {
        logging.DEBUG: f"{colorama.Fore.YELLOW}{format}{colorama.Style.RESET_ALL}",
        logging.INFO: f"{colorama.Fore.LIGHTGREEN_EX}{format}{colorama.Style.RESET_ALL}",
        logging.WARNING: f"{colorama.Fore.LIGHTRED_EX}{format}{colorama.Style.RESET_ALL}",
        logging.ERROR: f"{colorama.Fore.RED}{format}{colorama.Style.RESET_ALL}",
        logging.CRITICAL: f"{colorama.Fore.RED}{format}{format}{colorama.Style.RESET_ALL}",
    }

    def format(self, record) -> str:
        """See https://stackoverflow.com/a/384125"""
        record = copy.copy(record)
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def color_logging(level: int) -> logging.StreamHandler:
    """See https://docs.python.org/3/howto/logging-cookbook.html#logging-cookbook"""

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(level)
    # set a format which is simpler for console use
    formatter = LoggingFormatter()
    # tell the handler to use this format
    console.setFormatter(formatter)
    return console


def file_exists(dir_path: str | os.PathLike, name: str) -> bool:
    """Returns true if the path exists"""
    path_to_name = pathlib.Path(os.path.join(dir_path, name))
    if path_to_name.exists():
        return True
    else:
        return False
