import logging

import colorama

from core import authenticate, send_emails, parse_excel_file, iterate_pandas_rows
from helper import color_logging
from constants import SUBJECT_FIRST_EMAIL

logger = logging.getLogger(__name__)


def main():
    """
    Authenticates and sends the emails
    See: https://developers.google.com/gmail/api/quickstart/python
    """

    console = color_logging(level=logging.DEBUG)
    logging.basicConfig(
        level=logging.DEBUG,
        force=True,
        handlers=[console],
    )  # Force is needed here to re config logging
    # Init should be here so as the colors be rendered properly in fly.io
    colorama.init(convert=True)

    creds = authenticate()
    excel_file_emails = parse_excel_file()
    it = iterate_pandas_rows(df=excel_file_emails)
    send_emails(creds=creds, it=it, subject=SUBJECT_FIRST_EMAIL)


if __name__ == "__main__":
    main()
