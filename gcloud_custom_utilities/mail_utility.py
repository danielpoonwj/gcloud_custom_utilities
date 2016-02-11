import os
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


def send_mail(
            send_to,
            subject,
            text,
            send_from=None,
            username=None,
            password=None,
            server='smtp.gmail.com',
            port=587,
            isTls=True):

    if username is None or password is None:
        DEFAULT_CREDENTIAL_PATH = os.path.join(os.path.expanduser('~'), 'mail_credentials.json')

        if os.path.exists(DEFAULT_CREDENTIAL_PATH):
            DEFAULT_CREDENTIALS = json.loads(open(DEFAULT_CREDENTIAL_PATH, 'r').read())
            username = DEFAULT_CREDENTIALS['mail']['username']
            password = DEFAULT_CREDENTIALS['mail']['password']

    assert username is not None
    assert password is not None

    if send_from is None:
        send_from = username

    if not isinstance(send_to, list):
        send_to = [send_to]

    message = MIMEMultipart()
    message['From'] = send_from
    message['To'] = COMMASPACE.join(send_to)
    message['Date'] = formatdate(localtime=True)
    message['Subject'] = subject

    message.attach(MIMEText(text))

    smtp = smtplib.SMTP(server, port)
    if isTls:
        smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg=message.as_string())
    smtp.quit()
