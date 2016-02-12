import os
import sys
import shutil
import git
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import logging
from StringIO import StringIO


class _Progress(git.RemoteProgress):
    def line_dropped(self, line):
        print line

    def update(self, *args):
        print self._cur_line


def git_clone(git_url, write_dir, overwrite_warning=True):
    if os.path.isdir(write_dir):
        if overwrite_warning:
            print 'WARNING: Folder exists.\n\t%s\n\nDelete folder and continue?' % write_dir
            response = raw_input('[y/n]: ').strip().lower()
            while response not in ('y', 'n'):
                response = raw_input('Response').strip().lower()

            if response == 'n':
                sys.exit('Git Clone cancelled')

        print '\nExisting Folder Deleted\n'
        shutil.rmtree(write_dir)

    os.mkdir(write_dir)
    git.Repo.clone_from(git_url, write_dir, progress=_Progress())


def git_fetch(repo_dir):
    repo = git.Repo(repo_dir)
    origin = repo.remote(name='origin')

    for fetch_info in origin.fetch(progress=_Progress()):
        print 'Updated %s to %s' % (fetch_info.ref, fetch_info.commit)
        print


class StringLogger:
    def __init__(self, name=None, level=logging.DEBUG, formatter=None):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        if formatter is None:
            self._formatter = logging.Formatter(
                    fmt='%(asctime)s [%(levelname)s]: %(message)s',
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
