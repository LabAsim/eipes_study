import logging

import colorama

from core import (
    authenticate,
    send_emails,
    parse_excel_file,
    iterate_pandas_rows,
    drop_email_duplicates,
)
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
    )
    colorama.init(convert=True)
    creds = authenticate()
    excel_file_emails = parse_excel_file()
    excel_file_emails = drop_email_duplicates(df=excel_file_emails)
    it = iterate_pandas_rows(df=excel_file_emails)
    send_emails(creds=creds, it=it, subject=SUBJECT_FIRST_EMAIL)


if __name__ == "__main__":
    main()
