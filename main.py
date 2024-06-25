import logging

import colorama

from core import (
    authenticate,
    send_emails,
    parse_excel_file,
    iterate_pandas_rows,
    drop_email_duplicates,
    compare_save_emails_locally,
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
    # https://support.google.com/mail/answer/22839?hl=en#zippy=%2Cyou-have-reached-a-limit-for-sending-mail
    # You may see this message if you email a total of more than 500 recipients in a single email
    # and or more than 500 emails sent in a day.
    _input = input("\nAre you sure that you checked that these emails are <=500?\n")
    if _input not in ("yes", "Yes", "1", "y", "Y"):
        logger.error("Double check the emails!")
        return
    excel_file_emails = parse_excel_file()
    excel_file_emails = drop_email_duplicates(df=excel_file_emails)
    excel_file_emails = compare_save_emails_locally(df=excel_file_emails)
    it = iterate_pandas_rows(df=excel_file_emails)
    send_emails(creds=creds, it=it, subject=SUBJECT_FIRST_EMAIL)


if __name__ == "__main__":
    main()
