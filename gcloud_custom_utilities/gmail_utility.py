import os
from datetime import datetime
import httplib2
from oauth2client.tools import run_flow, argparser
from oauth2client.client import flow_from_clientsecrets, UnknownClientSecretsFlowError
from oauth2client.contrib import multistore_file
from googleapiclient.discovery import build

from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
import mimetypes
from email.utils import COMMASPACE

from tabulate import tabulate

from pytz import timezone
import base64
import shutil


def generate_email_search_query(
        has_attachment=True,
        mailbox_label=None,
        subject=None,
        sent_from=None,
        file_name=None,
        start_date=None,
        end_date=None):

    if has_attachment:
        search_filter = 'has:attachment'
    else:
        search_filter = ''

    if mailbox_label is not None:
        if isinstance(sent_from, list):
            search_filter += ' OR'.join([' label:%s' % x for x in mailbox_label])
        else:
            search_filter += ' label:%s' % mailbox_label

    if subject is not None:
        if isinstance(subject, list):
            search_filter += ' OR'.join([' subject:%s' % x for x in subject])
        else:
            search_filter += ' subject:%s' % subject

    if sent_from is not None:
        if isinstance(sent_from, list):
            search_filter += ' OR'.join([' from:%s' % x for x in sent_from])
        else:
            search_filter += ' from:%s' % sent_from

    if file_name is not None:
        if isinstance(file_name, list):
            search_filter += ' OR'.join([' filename:%s' % x for x in file_name])
        else:
            search_filter += ' filename:%s' % file_name

    if start_date is not None:
        try:
            date_after = start_date.strftime('%Y/%m/%d')
        except AttributeError:
            date_after = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y/%m/%d')
        search_filter += ' after:%s' % date_after

    if end_date is not None:
        try:
            date_before = end_date.strftime('%Y/%m/%d')
        except AttributeError:
            date_before = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y/%m/%d')
        search_filter += ' before:%s' % date_before

    return search_filter


class GmailUtility:
    def __init__(self, user_name, credential_file_path, client_secret_path=None, logger=None):
        try:
            import argparse
            flags = argparse.ArgumentParser(parents=[argparser]).parse_args()
        except ImportError:
            flags = None

        OAUTH_SCOPE = 'https://mail.google.com/'

        storage = multistore_file.get_credential_storage(filename=credential_file_path, client_id=user_name, user_agent=None, scope=OAUTH_SCOPE)
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            if client_secret_path is None or not os.path.exists(client_secret_path):
                raise UnknownClientSecretsFlowError('Credentials unavailable. Please provide a valid client_secret_path to rerun authentication')

            # Run through the OAuth flow and retrieve credentials
            FLOW = flow_from_clientsecrets(client_secret_path, scope=OAUTH_SCOPE)
            credentials = run_flow(FLOW, storage, flags)

        # Create an httplib2.Http object and authorize it with our credentials
        http = httplib2.Http()
        http = credentials.authorize(http)

        service = build('gmail', 'v1', http=http)

        self._service = service
        self._user = self._service.users()
        self._messages = self._user.messages()
        self._drafts = self._user.drafts()

        self._logger = logger

    def get_user_profile(self):
        return self._user.getProfile(userId='me').execute()

    def list_messages(self, include_all=False, query=None, max_results=None, show_full_messages=True):
        message_list = []
        message_count = 0

        response = self._messages.list(
            userId='me',
            includeSpamTrash=include_all,
            q=query
        ).execute()

        if 'messages' in response:
            message_list += response['messages']
            message_count += len(response['messages'])
        else:
            return message_list

        if message_count > max_results:
            message_list = message_list[:max_results]
        else:
            while 'nextPageToken' in response:
                page_token = None
                if 'nextPageToken' in response:
                    page_token = response['nextPageToken']

                response = self._messages.list(
                    userId='me',
                    includeSpamTrash=include_all,
                    q=query,
                    pageToken=page_token
                ).execute()

                if 'messages' in response:
                    message_list += response['messages']
                    message_count += len(response['messages'])

                if message_count > max_results:
                    message_list = message_list[:max_results]
                    break

        if show_full_messages:
            message_list = [self._get_message(x['id'], format='full') for x in message_list]

        return message_list

    def list_drafts(self, include_all=False, max_results=None, show_full_messages=True):
        draft_list = []
        draft_count = 0

        response = self._drafts.list(
            userId='me',
            includeSpamTrash=include_all
        ).execute()

        if 'drafts' in response:
            draft_list += response['drafts']
            draft_count += len(response['drafts'])
        else:
            return draft_list

        if draft_count > max_results:
            draft_list = draft_list[:max_results]
        else:
            while 'nextPageToken' in response:
                page_token = None
                if 'nextPageToken' in response:
                    page_token = response['nextPageToken']

                response = self._drafts.list(
                    userId='me',
                    includeSpamTrash=include_all,
                    pageToken=page_token
                ).execute()

                if 'drafts' in response:
                    draft_list += response['drafts']
                    draft_count += len(response['drafts'])

                if draft_count > max_results:
                    draft_list = draft_list[:max_results]
                    break

        if show_full_messages:
            draft_list = [self._get_draft(x['id'], format='full') for x in draft_list]

        return draft_list

    def _get_message(self, id, format='full'):
        return self._messages.get(id=id, userId='me', format=format).execute()

    def _get_attachment(self, attachment_id, message_id):
        return self._messages.attachments().get(id=attachment_id, messageId=message_id, userId='me').execute()

    def _get_draft(self, id, format='full'):
        return self._drafts.get(id=id, userId='me', format=format).execute()

    def download_email_attachments(
            self,
            write_dir,
            attachment_filter=None,
            clear_write_dir=False,
            output_heirarchy=None,
            search_query=None,
            print_details=True):

        # make directory if not exists
        if not os.path.exists(write_dir):
            os.makedirs(write_dir)

        # recursively clears write_dir of files and folders if option selected
        # will automatically be disabled if write_dir is root folder
        if clear_write_dir and write_dir != os.path.dirname(os.path.realpath(__file__)):
            for subfolder in next(os.walk(write_dir))[1]:
                if not subfolder.startswith('.'):
                    shutil.rmtree(os.path.join(write_dir, subfolder))

            for file in os.listdir(write_dir):
                if not file.startswith('.'):
                    os.remove(os.path.join(write_dir, file))

        if output_heirarchy is not None:
            if isinstance(output_heirarchy, str):
                output_heirarchy = [output_heirarchy.lower()]
            elif isinstance(output_heirarchy, list):
                output_heirarchy = [x.lower() for x in output_heirarchy]
            else:
                output_heirarchy = None

        if attachment_filter is not None:
            if not isinstance(attachment_filter, list):
                attachment_filter = [attachment_filter]

        def _list_attachments(obj, key, parts_list):
            if isinstance(obj, dict):
                try:
                    parts_list.append({'filename': obj['filename'], 'attachmentId': obj['body']['attachmentId']})
                except KeyError:
                    pass

            if key in obj and isinstance(obj[key], list):
                for part in obj[key]:
                    try:
                        parts_list.append({'filename': part['filename'], 'attachmentId': part['body']['attachmentId']})
                    except KeyError:
                        pass

                    if key in part:
                        _list_attachments(part, key, parts_list)

        mail_list = self.list_messages(query=search_query, max_results=100000)

        for mail in mail_list:
            mail_id = mail['id']
            mail_meta = {x['name'].lower(): x['value'] for x in mail['payload']['headers']}
            mail_meta['date'] = datetime.fromtimestamp(float(mail['internalDate'])/1000, timezone('Asia/Singapore')).replace(tzinfo=None)

            attachment_list = []
            _list_attachments(mail['payload'], 'parts', attachment_list)

            for attachment_dict in attachment_list:
                file_name = attachment_dict['filename']

                if attachment_filter is not None and not any([x in file_name for x in attachment_filter]):
                    continue

                mail_meta['extension'] = os.path.splitext(file_name)[-1].replace('.', '')

                attachment = self._get_attachment(attachment_dict['attachmentId'], mail_id)

                if output_heirarchy is None:
                    sub_write_dir = write_dir
                else:
                    sub_write_dir = os.path.join(write_dir, *[mail_meta[x].strftime('%Y%m%d') if x == 'date' else mail_meta[x] for x in output_heirarchy])

                if not os.path.exists(sub_write_dir):
                    os.makedirs(sub_write_dir)

                write_path = os.path.join(sub_write_dir, file_name)

                # check if file already exists, if it does, append counter and write
                counter = 1
                while os.path.exists(write_path):
                    file_original_name = os.path.splitext(file_name)[0]
                    file_original_ext = os.path.splitext(file_name)[1]
                    write_path = os.path.join(sub_write_dir, '%s-%d%s' % (file_original_name, counter, file_original_ext))
                    counter += 1

                file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))

                with open(write_path, 'wb') as write_file:
                    write_file.write(file_data)

                logging_string = '[Gmail] Downloaded %s from [%s]: %s (%s)' % (
                        file_name,
                        mail_meta['from'],
                        mail_meta['subject'],
                        mail_meta['date']
                    )

                if print_details:
                    print '\t%s' % logging_string

                if self._logger is not None:
                    self._logger.info(logging_string)

    def convert_list_to_html(self, data, has_header=True):
        if has_header:
            header = data.pop(0)
        else:
            header = ()

        table_html = tabulate(data, headers=header, tablefmt='html')

        table_html = table_html.replace('<table>', '<table style="width: 100%; border-collapse: collapse; border: 2px solid black;">')
        table_html = table_html.replace('<td>', '<td style="border: 1px solid black;">')
        table_html = table_html.replace('<th>', '<th style="border: 2px solid black;">')

        return table_html

    def _create_message(self, sender, to, subject, message_text, attachment_file_paths):
        def __generate_msg_part(part):
            assert isinstance(part, dict)
            assert 'text' in part
            if 'type' not in part:
                msg_type = 'plain'
            else:
                msg_type = part['type']

            mime_part = MIMEText(part['text'].encode('utf-8'), _subtype=msg_type, _charset='utf-8')
            return mime_part

        if not isinstance(to, list):
            to = [to]

        if message_text is None or isinstance(message_text, (unicode, str)):
            msgs = [MIMEText(message_text.encode('utf-8'), _charset='utf-8')]
        elif isinstance(message_text, dict):
            msgs = [__generate_msg_part(message_text)]
        elif isinstance(message_text, list):
            msgs = [__generate_msg_part(message_part) for message_part in message_text]
        else:
            raise TypeError('Types accepted for message_text: string, dict, or list of dicts')

        message = MIMEMultipart()
        message['to'] = COMMASPACE.join(to)
        message['from'] = sender
        message['subject'] = subject

        # separate part for text etc
        message_alt = MIMEMultipart('alternative')
        message.attach(message_alt)

        # html portions have to be under a separate 'related' part under 'alternative' part
        # sequence matters, text > related (html > inline image) > attachments. ascending priority
        # if message text is a list, it's providing alternatives.
        # eg. if both plain and html are available, Gmail will choose HTML over plain

        # attach text first (lower priority)
        plain_msgs = [x for x in msgs if x.get_content_subtype() == 'plain']
        for msg in plain_msgs:
            message_alt.attach(msg)

        # create 'related' part if html is required
        content_msgs = [x for x in msgs if x.get_content_subtype() == 'html' or x.get_content_maintype() == 'image']

        if len(content_msgs) > 0:
            message_related = MIMEMultipart('related')
            message_alt.attach(message_related)

            for msg in content_msgs:
                message_related.attach(msg)

        # different treatment if contains attachments
        if attachment_file_paths is not None:
            if isinstance(attachment_file_paths, str):
                attachment_file_paths = [attachment_file_paths]
            elif not isinstance(attachment_file_paths, list):
                raise TypeError('Invalid input for attachment_file_paths. Only acceptable types are str and list objects')

            for file_path in attachment_file_paths:

                assert os.path.exists(file_path)

                content_type, encoding = mimetypes.guess_type(file_path)

                if content_type is None or encoding is not None:
                    content_type = 'application/octet-stream'

                main_type, sub_type = content_type.split('/', 1)

                if main_type == 'text':
                    with open(file_path, 'rb') as fp:
                        msg = MIMEText(fp.read(), _subtype=sub_type)

                elif main_type == 'image':
                    with open(file_path, 'rb') as fp:
                        msg = MIMEImage(fp.read(), _subtype=sub_type)

                else:
                    with open(file_path, 'rb') as fp:
                        msg = MIMEBase(main_type, sub_type)
                        msg.set_payload(fp.read())

                msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
                message.attach(msg)

        return {'raw': base64.urlsafe_b64encode(message.as_string())}

    def create_draft(self, sender, to, subject, message_text, attachment_file_paths=None):
        message = {'message': self._create_message(sender, to, subject, message_text, attachment_file_paths)}
        response = self._drafts.create(userId='me', body=message).execute()

        return response

    def send_email(self, sender, to, subject, message_text, attachment_file_paths=None):
        message = self._create_message(sender, to, subject, message_text, attachment_file_paths)
        response = self._messages.send(userId='me', body=message).execute()

        return response
