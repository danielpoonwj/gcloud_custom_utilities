import os
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging
from StringIO import StringIO


class StringLogger:
    def __init__(self, name=None, level=logging.DEBUG, formatter=None, ignore_modules=None):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        if formatter is None:
            self._formatter = logging.Formatter(
                    fmt='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            assert isinstance(formatter, logging.Formatter)
            self._formatter = formatter

        self._log_capture_string = StringIO()
        sh = logging.StreamHandler(self._log_capture_string)
        sh.setLevel(level)
        sh.setFormatter(self._formatter)

        self._logger.addHandler(sh)

        # filters logging for modules in ignore_modules
        if ignore_modules is not None and isinstance(ignore_modules, (list, tuple)):
            class _LoggingFilter(logging.Filter):
                def filter(self, record):
                    # still lets WARNING, ERROR and CRITICAL through
                    return record.name not in ignore_modules or \
                           record.levelno in (logging.WARNING, logging.ERROR, logging.CRITICAL)

            for handler in self._logger.handlers:
                handler.addFilter(_LoggingFilter())

    def get_logger(self):
        return self._logger

    def get_log_string(self):
        return self._log_capture_string.getvalue()

    def close(self):
        self._log_capture_string.close()


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
