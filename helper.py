import copy
import logging
import os
import pathlib

import colorama
import pandas as pd

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


def sanitise_log_emails() -> pd.DataFrame:
    """Sanitise the entries"""
    df = pd.read_csv("emails.log", header=None)
    df[0] = df[0].str.replace("_email=", "")

    return df


def remove_logged_emails_from_saved_xlsx() -> None:
    """Removes any emails found in the emails.log from emails_sent.xlsx"""
    df = pd.read_excel(io="emails_sent.xlsx")

    to_compare = sanitise_log_emails()

    df["bools"] = False

    for ind, mail in pd.DataFrame(df["emails"]).itertuples():
        # print(f"{ind=}:{mail}")

        for sub_ind, sub_mail in to_compare.itertuples():
            # print(sub_mail)
            if str(mail) in sub_mail:
                df.loc[ind, "bools"] = True

    print(df.loc[df["bools"] is True].to_string())

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    df["emails"] = df.query("bools == False")["emails"]

    print(df.tail())

    # Drop NAs and save the Excel file
    df.dropna().to_excel("emails_sent.xlsx", columns=["emails"], index=False)
