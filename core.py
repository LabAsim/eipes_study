from __future__ import print_function
import base64
import datetime
import inspect
import io
import logging
import os.path
import pickle
import random
import shutil
import ssl
import time
from email.message import EmailMessage
from mimetypes import MimeTypes
from typing import Iterator, NamedTuple, Any
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from constants import SCOPES, CONTENT_THANKING
from helper import file_exists
from saved_tokens import GMAIL_USERNAME

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
    # Remove leading/trailing whitespaces
    excel_file["emails"] = excel_file["emails"].str.strip().str.lower()
    # Create the personalized links
    non_prefilled_link = "https://docs.google.com/forms/d/e/1FAIpQLSdOuMQzg9iKtNZK4X-vzXkwttezx6Y9g5UhL5BLISYZ7cNLdA/viewform?usp=pp_url&entry.1713466637="  # noqa: E501 # ends withmail@mail.com
    excel_file["links"] = non_prefilled_link + excel_file["emails"]

    return excel_file


def drop_email_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drops the duplicates from emails column and returns the df"""
    df["emails"] = df["emails"].str.lower().str.strip()
    return df.drop_duplicates(subset=["emails"])


def compare_save_emails_locally(df: pd.DataFrame, excel_name: str) -> pd.DataFrame:
    """Checks if there is a file with the emails that we had sent the survey to.
    If there is not, it writes one to disk.
    If there is, it compares `emails.xlsx` values to `emails_sent.xlsx`.
    If an email of `emails.xlsx` is not present in  `emails_sent.xlsx`, it will be in the df returned

    :return A dataframe containing the emails and their corresponding links
    """

    emails_sent_excel_path = os.path.join(os.path.dirname(__file__), excel_name)

    if file_exists(dir_path=os.path.dirname(__file__), name=excel_name):
        emails_sent = pd.read_excel(io=emails_sent_excel_path, engine="openpyxl")
        # Remove leading/trailing whitespaces
        emails_sent["emails"] = emails_sent["emails"].str.lower().str.strip()
        df["emails"] = df["emails"].str.lower().str.strip()

        df["emails_bool"] = df["emails"].isin(emails_sent["emails"])

        df = pd.concat(objs=[emails_sent, df]).drop_duplicates(subset=["emails"])
        df = drop_email_duplicates(df=df)
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html
        df.drop_duplicates(inplace=True)

        # Re-write the emails_sent file with the old + new emails
        df.to_excel(excel_writer=emails_sent_excel_path, columns=["emails"], index=False)
        logger.debug(f"Saving to '{emails_sent_excel_path}'")
        if "links" in df.columns:
            df_to_return = pd.DataFrame(  # noqa
                df.loc[df["emails_bool"] == False, ["emails", "links"]]  # noqa
            )  # noqa

        else:
            df_to_return = pd.DataFrame(df.loc[df["emails_bool"] == False, ["emails"]])  # noqa

        logger.debug(f"{df_to_return.shape=}")
        logger.debug(f"{df_to_return.to_string()}")
        return df_to_return

    else:
        df["emails"] = df["emails"].str.strip().str.lower()
        df.to_excel(excel_writer=emails_sent_excel_path, columns=["emails"], index=False)
        logger.debug(f"{df.shape=}")
        logger.debug(f"{df.to_string()}")

        return (
            pd.DataFrame(df[["emails", "links"]])
            if "links" in df.columns
            else pd.DataFrame(df[["emails"]])
        )


def iterate_pandas_rows(df: pd.DataFrame) -> Iterator:
    """
    itertuples() returns a NamedTuple
    See: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.itertuples.html#pandas.DataFrame.itertuples  # noqa: E501
    """

    for row in df.itertuples():
        row: NamedTuple[tuple[Any, ...]] = row

        yield row.emails, row.links


def iterate_pandas_single_column(df: pd.DataFrame) -> Iterator:
    """
    itertuples() returns a NamedTuple
    See: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.itertuples.html#pandas.DataFrame.itertuples  # noqa: E501
    """

    for row in df.itertuples():
        row: NamedTuple[tuple[Any, ...]] = row

        yield row.emails, None


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


def choose_random_content(link: str) -> str:
    """Returns randomly one out of 3 templates"""
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

    return content


def send_emails(creds: Credentials, it: Iterator, subject: str) -> None:
    """
    Sends the emails
    See the gmail API: https://developers.google.com/gmail/api/reference/rest/v1/users.messages
    """

    # create gmail api client
    service = build("gmail", "v1", credentials=creds)
    counter = 1
    for _email, link in it:
        try:
            message = EmailMessage()  # policy=policy.EmailPolicy(cte_type="8bit", utf8=True)
            logger.debug(f"{_email=}")
            logger.debug(f"{counter=}")
            counter += 1
            # message.set_content(content)
            # https://stackoverflow.com/a/16906974
            content = choose_random_content(link=link)
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
            random_time = random.randrange(start=60, stop=70, step=1)
            logger.info(f"Message sent successfully. Sleeping for {random_time=}")
            time.sleep(random_time)
        except ssl.SSLEOFError as err:
            logger_email.error(
                f"# {inspect.getframeinfo(inspect.currentframe())[2]} SSLEOFError "
                f"({datetime.datetime.now()}) \n{_email=}\n"
            )
            logger.exception(f"{err=}")
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
    answers_file["emails"] = answers_file["emails"].str.strip().str.lower()
    excel_file["emails"] = excel_file["emails"].str.strip().str.lower()
    excel_file = drop_email_duplicates(df=excel_file)
    excel_file["emails_bool"] = excel_file["emails"].isin(answers_file["emails"])
    # See: https://sparkbyexamples.com/pandas/pandas-extract-column-value-based-on-another-column/#:~:text=Using%20DataFrame.,-Values()&text=value()%20property%2C%20you%20can,end%20to%20access%20the%20value.  # noqa: E501

    return pd.DataFrame(  # noqa
        excel_file.loc[excel_file["emails_bool"] == False, ["emails", "links"]]  # noqa
    )  # noqa


def extract_answered_emails() -> pd.DataFrame:
    """Extracts the emails from the answers"""

    answers_file = pd.read_excel(
        io="ΕΡΕΥΝΑ ΓΙΑ ΤΗΝ ΙΑΤΡΙΚΗ ΠΑΙΔΕΙΑ ΚΑΙ ΕΡΓΑΣΙΑ (ΕΙΠΕς) (Απαντήσεις).xlsx"
    )
    answers_file["emails"] = answers_file["Διεύθυνση ηλεκτρονικού ταχυδρομείου "]
    # See: https://sparkbyexamples.com/pandas/pandas-extract-column-value-based-on-another-column/#:~:text=Using%20DataFrame.,-Values()&text=value()%20property%2C%20you%20can,end%20to%20access%20the%20value.  # noqa: E501

    # This is redundant. It's only added for code compatibility.
    # answers_file["links"] = None

    return pd.DataFrame(answers_file["emails"])


def send_reminders(creds: Credentials, it: Iterator, subject: str) -> None:
    """
    Sends the reminder emails.
    """

    # create gmail api client
    service = build("gmail", "v1", credentials=creds)
    counter = 1
    for _email, link in it:
        try:
            message = EmailMessage()  # policy=policy.EmailPolicy(cte_type="8bit", utf8=True)
            logger.debug(f"{_email=}")
            logger.debug(f"{counter=}")
            counter += 1
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
                </body>
            </html>
            """
            # Use this for html
            message.add_header("Content-Type", "text/html")
            message.set_payload(content)
            message["Bcc"] = _email
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
            random_time = random.randrange(start=60, stop=70, step=1)
            logger.info(f"Message sent successfully. Sleeping for {random_time=}")
            time.sleep(random_time)
        except HttpError as err:
            logger_email.error(f"{_email=}")
            logger.exception(f"{err=}")
        except ssl.SSLEOFError as err:
            logger_email.error(
                f"# {inspect.getframeinfo(inspect.currentframe())[2]} SSLEOFError "
                f"({datetime.datetime.now()}) \n{_email=}\n"
            )
            logger.exception(f"{err=}")
        except Exception as err:
            logger.error("Exception")
            logger_email.error(f"{_email=}")
            logger.exception(f"{err=}")


def send_thanks(creds: Credentials, it: Iterator, subject: str) -> None:
    """
    Sends the thank-you emails.
    """

    # create gmail api client
    service = build("gmail", "v1", credentials=creds)
    counter = 1
    for _email, link in it:
        try:
            message = EmailMessage()  # policy=policy.EmailPolicy(cte_type="8bit", utf8=True)
            logger.debug(f"{_email=}")
            logger.debug(f"{link=}")
            logger.debug(f"{counter=}")
            counter += 1
            # message.set_content(content)
            # https://stackoverflow.com/a/16906974

            message.set_payload(CONTENT_THANKING)
            message["Bcc"] = _email
            message["From"] = "eipes.study@gmail.com"
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
            random_time = random.randrange(start=60, stop=70, step=1)
            logger.info(f"Message sent successfully. Sleeping for {random_time=}")
            time.sleep(random_time)
        except ssl.SSLEOFError as err:
            logger_email.error(
                f"# {inspect.getframeinfo(inspect.currentframe())[2]} SSLEOFError "
                f"({datetime.datetime.now()}) \n{_email=}\n"
            )
            logger.exception(f"{err=}")
        except (HttpError, Exception) as err:
            logger_email.error(f"{_email=}")
            logger.exception(f"{err=}")


class DriveAPI:
    """

    The class is modified.
    The source code taken from here:
    https://www.geeksforgeeks.org/upload-and-download-files-from-google-drive-storage-using-python/
    """

    # Define the scopes
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    def __init__(self, calling_from: str = "python"):

        # Variable self.creds will
        # store the user access token.
        # If no valid token found
        # we will create one.
        self.creds = None
        self.calling_from = calling_from
        # The file token.pickle stores the
        # user's access and refresh tokens. It is
        # created automatically when the authorization
        # flow completes for the first time.
        logger.debug(f"{os.path.dirname(__file__)=}")
        # Check if file token.pickle exists
        if os.path.exists("token.pickle"):
            # Read the token from the file and
            # store it in the variable self.creds
            with open("token.pickle", "rb") as token:
                self.creds = pickle.load(token)

            # If no valid credentials are available,
        # request the user to log in.
        if not self.creds or not self.creds.valid:

            # If token is expired, it will be refreshed,
            # else, we will request a new one.
            if (
                self.creds
                and self.creds.expired
                and self.creds.refresh_token
                and self.calling_from == "python"
            ):
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", DriveAPI.SCOPES
                )
                self.creds = flow.run_local_server(port=0)

            # Save the access token in token.pickle
            # file for future usage
            with open("token.pickle", "wb") as token:
                pickle.dump(self.creds, token)

            # Connect to the API service
        self.service = build("drive", "v3", credentials=self.creds)

        # request a list of first N files or
        # folders with name and id from the API.
        results = self.service.files().list(pageSize=100, fields="files(id, name)").execute()
        items = results.get("files", [])

        # print a list of files

        logger.debug("Here's a list of files: \n")
        for item in items:  # , sep="\n", end="\n\n"
            logger.debug(msg=item)
        else:
            logger.debug(msg="---------------")

    def download_file(self, file_id, file_name):
        """Downloads the selected file based on `file_id` and saves it as `file_name`"""

        # See here for mimeType: https://developers.google.com/drive/api/guides/ref-export-formats
        # It needs export_media with a certain mimeType, not get_media (this is only for binary content)
        # See example: https://developers.google.com/drive/api/guides/manage-downloads#export-content
        request = self.service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        fh = io.BytesIO()

        # Initialise a downloader object to download the file
        downloader = MediaIoBaseDownload(fh, request, chunksize=204800)
        done = False

        while done is False:
            status, done = downloader.next_chunk()
            logger.debug("Download %d%%." % int(status.progress() * 100))
        try:
            # Download the data in chunks
            while not done:
                status, done = downloader.next_chunk()

            fh.seek(0)

            # Write the received data to the file
            with open(file_name, "wb") as f:
                shutil.copyfileobj(fh, f)

            logger.info("File Downloaded")
            # Return True if file Downloaded successfully
            return True
        except Exception as err:
            logger.exception(f"{err=}")
            # Return False if something went wrong
            logger.exception("Something went wrong.")
            return False

    def FileUpload(self, filepath):

        # Extract the file name out of the file path
        name = filepath.split("/")[-1]

        # Find the MimeType of the file
        mimetype = MimeTypes().guess_type(name)[0]

        # create file metadata
        file_metadata = {"name": name}

        try:
            media = MediaFileUpload(filepath, mimetype=mimetype)

            # Create a new file in the Drive storage
            self.service.files().create(body=file_metadata, media_body=media, fields="id").execute()

            print("File Uploaded.")

        except Exception as err:
            logger.exception(f"{err=}")
            # Raise UploadError if file is not uploaded.
            raise Exception("Can't Upload File.")
