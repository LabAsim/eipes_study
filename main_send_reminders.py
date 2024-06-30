import logging

import colorama

from core import (
    authenticate,
    iterate_pandas_rows,
    send_reminders,
    compare_emails,
    compare_save_emails_locally,
)
from helper import color_logging
from constants import SUBJECT_REMINDER_EMAIL

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
    excel_file_emails = compare_emails()
    excel_file_emails = compare_save_emails_locally(
        df=excel_file_emails, excel_name="emails_sent_reminder.xlsx"
    )
    it = iterate_pandas_rows(df=excel_file_emails)
    send_reminders(creds=creds, it=it, subject=SUBJECT_REMINDER_EMAIL)


if __name__ == "__main__":
    main()
