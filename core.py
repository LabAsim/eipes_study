import logging
import os.path
import base64
import random
import time
from email.message import EmailMessage
from typing import Iterator, NamedTuple, Any

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
import pandas as pd

from helper import file_exists
from saved_tokens import GMAIL_USERNAME
from constants import SCOPES

logger = logging.getLogger(__name__)

# Create a separate email which logs the email that failed
# There is not a sure way to catch an email that fails.
# See https://stackoverflow.com/questions/53561296/python-correct-method-verify-if-email-exists
# See: https://stackoverflow.com/a/13733863
logger_email = logging.getLogger("logger_email")
fh = logging.FileHandler("emails.log")
fh.setLevel(logging.ERROR)
logger_email.addHandler(fh)
console_handler = logging.StreamHandler()
logger_email.addHandler(console_handler)


def parse_excel_file() -> pd.DataFrame:
    """
    See: https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
    """

    excel_file = pd.read_excel(io="emails.xlsx")
    # Create the personalized links
    non_prefilled_link = "https://docs.google.com/forms/d/e/1FAIpQLSdOuMQzg9iKtNZK4X-vzXkwttezx6Y9g5UhL5BLISYZ7cNLdA/viewform?usp=pp_url&entry.1713466637="  # noqa: E501 # ends withmail@mail.com
    excel_file["links"] = non_prefilled_link + excel_file["emails"]

    return excel_file


def drop_email_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drops the duplicates from emails column and returns the df"""
    return df.drop_duplicates(subset=["emails"])


def compare_save_emails_locally(df: pd.DataFrame) -> pd.DataFrame:
    """Checks if there is a file with the emails that we had sent the survey to.
    If there is not, it writes one to disk.
    If there is, it compares `emails.xlsx` values to `emails_sent.xlsx`.
    If an email of `emails.xlsx` is not present in  `emails_sent.xlsx`, it will in the df returned

    :return A dataframe contains the emails and their corresponding links
    """

    emails_sent_excel_path = os.path.join(os.path.dirname(__file__), "emails_sent.xlsx")

    if file_exists(dir_path=os.path.dirname(__file__), name="emails_sent.xlsx"):
        emails_sent = pd.read_excel(io=emails_sent_excel_path)

        df["emails_bool"] = df["emails"].isin(emails_sent["emails"])

        # Re-write the emails_sent file with the old + new emails
        df.to_excel(excel_writer=emails_sent_excel_path, columns=["emails"], index=False)

        df_to_return = pd.DataFrame(df.loc[df["emails_bool"] == False, ["emails", "links"]])  # noqa
        logger.debug(f"{df_to_return.shape=}")

        return df_to_return
    else:
        df.to_excel(excel_writer=emails_sent_excel_path, columns=["emails"], index=False)

        return pd.DataFrame(df[["emails", "links"]])


def iterate_pandas_rows(df: pd.DataFrame) -> Iterator:
    """
    itertuples() returns a NamedTuple
    See: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.itertuples.html#pandas.DataFrame.itertuples  # noqa: E501
    """

    for row in df.itertuples():
        row: NamedTuple[tuple[Any, ...]] = row

        yield row.emails, row.links


def authenticate() -> Credentials:
    """Stores the user's creds"""
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return creds


def send_emails(creds: Credentials, it: Iterator, subject: str) -> None:
    """
    Sends the emails
    See the gmail API: https://developers.google.com/gmail/api/reference/rest/v1/users.messages
    """

    # create gmail api client
    service = build("gmail", "v1", credentials=creds)

    for _email, link in it:
        try:
            message = EmailMessage()  # policy=policy.EmailPolicy(cte_type="8bit", utf8=True)
            logger.debug(f"{_email=}")
            # message.set_content(content)
            # https://stackoverflow.com/a/16906974
            content = f"""
            <html>
                <meta name="viewport" content="width=device-width, initial-scale=1" charset="utf-8">
                <style>
                </style>
                <body>
                    <div align='center'>
                        <a>
                            <img src="https://i.ibb.co/qskr3ZR/eipes-header.png" alt="logo header" style="width:500px;">
                            <br/>
                        </a>
                    </div>
                    <h1 style="text-align: center;">
                        Καλώς ήρθατε στην μελέτη <b>ΕΙΠΕς</b>
                    </h1>
                    <p style="font-size:18px">
                        Αγαπητές και αγαπητοί συνάδελφοι, η μελέτη γίνεται να καταλάβουμε καλύτερα και
                        να καταδείξουμε τι πιστεύουν οι ειδικευόμενες/οι & οι φοιτήτριες/ές Ιατρικής
                        για την κατάστασή τους
                        αλλά και για τις αλλαγές που δρομολογεί για την ειδικότητα η ηγεσία του Υπουργείου Υγείας.
                    </p>
                    <p style="font-size:18px">
                        Παρακαλούμε απαντήστε σε όλες τις ερωτήσεις όσο καλύτερα μπορείτε.
                    </p>
                    <p style="font-size:18px">
                        Χρόνος συμπλήρωσης: 5 λεπτά
                    </p>
                    <h2 style="text-align: center;">
                        Πατήστε στον <a href="{link}">σύνδεσμο</a> για να συμμετέχετε στην έρευνα.
                    </h2>
                </body>
            </html>
            """
            content2 = f"""
            <html>
                <meta name="viewport" content="width=device-width, initial-scale=1" charset="utf-8">
                <style>
                </style>
                <body>
                    <div align='center'>
                        <a>
                            <img src="https://i.ibb.co/qskr3ZR/eipes-header.png" alt="logo header" style="width:500px;">
                            <br/>
                        </a>
                    </div>
                    <h1 style="text-align: center;">
                        Καλώς ήρθατε στην μελέτη <b>ΕΙΠΕς</b>
                    </h1>
                </body>
            </html>

            Αγαπητές και αγαπητοί συνάδελφοι, η μελέτη γίνεται να καταλάβουμε καλύτερα και
            να καταδείξουμε τι πιστεύουν οι ειδικευόμενες/οι & οι φοιτήτριες/ές Ιατρικής
            για την κατάστασή τους
            αλλά και για τις αλλαγές που δρομολογεί για την ειδικότητα η ηγεσία του Υπουργείου Υγείας.

            <html>
                <body>
                    <p style="font-size:18px">
                        Παρακαλούμε απαντήστε σε όλες τις ερωτήσεις όσο καλύτερα μπορείτε.
                    </p>
                    <p style="font-size:18px">
                        Χρόνος συμπλήρωσης: 5 λεπτά
                    </p>
                    <h2 style="text-align: center;">
                        Πατήστε στον <a href="{link}">σύνδεσμο</a> για να συμμετέχετε στην έρευνα.
                    </h2>
                </body>
            </html>
            """

            content3 = f"""
            <html>
                <meta name="viewport" content="width=device-width, initial-scale=1" charset="utf-8">
                <style>
                </style>
                <body>
                    <div align='center'>
                        <a>
                            <img src="https://i.ibb.co/qskr3ZR/eipes-header.png" alt="logo header" style="width:500px;">
                            <br/>
                        </a>
                    </div>
                    <h1 style="text-align: center;">
                        Καλώς ήρθατε στην μελέτη <b>ΕΙΠΕς</b>
                    </h1>
                </body>
            </html>

            Αγαπητές και αγαπητοί συνάδελφοι, η μελέτη γίνεται να καταλάβουμε καλύτερα και
            να καταδείξουμε τι πιστεύουν οι ειδικευόμενες/οι & οι φοιτήτριες/ές Ιατρικής
            για την κατάστασή τους
            αλλά και για τις αλλαγές που δρομολογεί για την ειδικότητα η ηγεσία του Υπουργείου Υγείας.

            <html>
                <body>
                    <h2 style="text-align: center;">
                        Πατήστε στον <a href="{link}">σύνδεσμο</a> για να συμμετέχετε στην έρευνα.
                    </h2>
                </body>
            </html>

            Χρόνος συμπλήρωσης: 5 λεπτά. Παρακαλούμε απαντήστε σε όλες τις ερωτήσεις όσο καλύτερα μπορείτε.
            """
            content = random.choice([content, content2, content3])
            # Use this for html
            message.add_header("Content-Type", "text/html")
            message.set_payload(content)
            message["Bcc"] = (
                _email  # Do not use "To", this will reveal the emails to every recipient
            )
            message["From"] = GMAIL_USERNAME
            message["Subject"] = subject
            encoded_message = base64.urlsafe_b64encode(
                message.as_bytes()  # policy=policy.EmailPolicy(cte_type="8bit", utf8=True)
            ).decode()

            create_message = {"raw": encoded_message}
            # pylint: disable=E1101
            # In order for this function to work, I have modified the line 409 in the generator.py of email library
            # I have replaced ascii with utf-8
            # The modified line: self._fp.write(s.encode('utf-8', 'surrogateescape'))
            # I didn't find another way to send a html with greek letters
            send_message = (
                service.users().messages().send(userId="me", body=create_message).execute()
            )

            logger.debug(f'Message Id: {send_message["id"]}')
            logger.debug(f"{send_message=}")
            random_time = random.randrange(start=60, stop=120, step=1)
            logger.info(f"Message sent successfully. Sleeping for {random_time=}")
            time.sleep(random_time)
        except (HttpError, Exception) as err:
            logger_email.error(f"{_email=}")
            logger.exception(f"{err=}")


def compare_emails() -> pd.DataFrame:
    """
    Extracts the emails and their corresponding links that point to people that have not answered in our survey
    (If someone has answered successfully, then their email will appear in the `emails` column)
    """
    excel_file = parse_excel_file()
    excel_file = drop_email_duplicates(df=excel_file)
    answers_file = pd.read_excel(
        io="ΕΡΕΥΝΑ ΓΙΑ ΤΗΝ ΙΑΤΡΙΚΗ ΠΑΙΔΕΙΑ ΚΑΙ ΕΡΓΑΣΙΑ (ΕΙΠΕς) (Απαντήσεις).xlsx"
    )
    answers_file["emails"] = answers_file["Διεύθυνση ηλεκτρονικού ταχυδρομείου "]
    excel_file["emails_bool"] = excel_file["emails"].isin(answers_file["emails"])
    # See: https://sparkbyexamples.com/pandas/pandas-extract-column-value-based-on-another-column/#:~:text=Using%20DataFrame.,-Values()&text=value()%20property%2C%20you%20can,end%20to%20access%20the%20value.  # noqa: E501

    return pd.DataFrame(  # noqa
        excel_file.loc[excel_file["emails_bool"] == False, ["emails", "links"]]  # noqa
    )  # noqa


def send_reminders(creds: Credentials, it: Iterator, subject: str) -> None:
    """
    Sends the reminder emails.
    """

    # create gmail api client
    service = build("gmail", "v1", credentials=creds)
    _input = input(
        "\nAre you sure that you checked that these emails are the ones that didn't answer and they are <=500?\n"
    )
    if _input not in ("yes", "Yes", "1", "y", "Y"):
        logger.error("Double check the emails!")
        return
    for _email, link in it:
        try:
            message = EmailMessage()  # policy=policy.EmailPolicy(cte_type="8bit", utf8=True)
            logger.debug(f"{_email=}")
            # message.set_content(content)
            # https://stackoverflow.com/a/16906974
            content = f"""
            <html>
                <meta name="viewport" content="width=device-width, initial-scale=1" charset="utf-8">
                <style>
                </style>
                <body>
                    <div align='center'>
                        <a>
                            <img src="https://i.ibb.co/qskr3ZR/eipes-header.png" alt="logo header" style="width:500px;">
                            <br/>
                        </a>
                    </div>
                    <h1 style="text-align: center;">
                        <strong><u>Υπενθύμιση</u></strong> για να συμμετέχετε στην μελέτη <b>ΕΙΠΕς</b>

                    </h1>
                    <h2 style="text-align: center;">
                        Πατήστε στον <a href="{link}">σύνδεσμο</a> που ακολουθεί για να συμμετέχετε στην έρευνα.
                    </h2>
                    <h3 style="text-align: center;">
                        {link}
                    </h3>
                </body>
            </html>
            """
            # Use this for html
            message.add_header("Content-Type", "text/html")
            message.set_payload(content)
            message["Bcc"] = (
                _email  # Do not use "To", this will reveal the emails to every recipient
            )
            message["From"] = GMAIL_USERNAME
            message["Subject"] = subject
            encoded_message = base64.urlsafe_b64encode(
                message.as_bytes()  # policy=policy.EmailPolicy(cte_type="8bit", utf8=True)
            ).decode()

            create_message = {"raw": encoded_message}
            # pylint: disable=E1101
            # In order for this function to work, I have modified the line 409 in the generator.py of email library
            # I have replaced ascii with utf-8
            # The modified line: self._fp.write(s.encode('utf-8', 'surrogateescape'))
            # I didn't find another way to send an html with greek letters
            send_message = (
                service.users().messages().send(userId="me", body=create_message).execute()
            )

            logger.debug(f'Message Id: {send_message["id"]}')
            logger.debug(f"{send_message=}")
            random_time = random.randrange(start=60, stop=120, step=1)
            logger.info(f"Message sent successfully. Sleeping for {random_time=}")
            time.sleep(random_time)
        except (HttpError, Exception) as err:
            logger_email.error(f"{_email=}")
            logger.exception(f"{err=}")
