"""
Abstracts away much of what's needed for read-only access to Google Drive via
its API, simplifying the common case of reading data from documents,
spreadsheets, and downloading images.

DriveClient instances contain a service property which can be used to access
the full API as the authenticated user. In order to make changes, set an
appropriate read/write scope when instantiating a client.
"""

import argparse
import csv
import os
import json
from functools import partial

import httplib2
import oauth2client
from apiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client import client, tools


CLIENT_SECRET_FILENAME = 'client_secret.json'
CACHED_CREDENTIALS_FILENAME = 'drive_client.json'
SCOPES = 'https://www.googleapis.com/auth/drive.readonly'


class DriveClient(object):
    '''
    This object handles the connection to Google's Drive API and provides basic
    methods to fetch files or folders by id or a custom query. Shortcuts are
    provided for the common case of fetching a file or folder by name or id.
    '''
    def __init__(self, name, client_secret_filename=CLIENT_SECRET_FILENAME,
                 cached_credentials_filename=CACHED_CREDENTIALS_FILENAME,
                 scopes=SCOPES, service_account_json_filename=None):
        '''
        If a service_account_json_filename is provided, a private key will be
        used instead of the user-assisted OAuth flow which requires a browser.
        '''
        self.name = name
        self.client_secret_filename = client_secret_filename

        if os.path.isabs(cached_credentials_filename):
            self.cached_credentials_path = cached_credentials_filename
        else:
            credential_dir = os.path.join(os.path.expanduser('~'), '.credentials')
            if not os.path.exists(credential_dir):
                os.makedirs(credential_dir)
            self.cached_credentials_path = os.path.join(credential_dir, cached_credentials_filename)

        self.scopes = scopes
        self.service_account_json_filename = service_account_json_filename
        self.flags,_ = argparse.ArgumentParser(parents=[tools.argparser]).parse_known_args()

    @property
    def http(self):
        '''
        This process is handled entirely by the credentials object from the
        oauth2client library.
        '''
        try: self._http
        except AttributeError: self._http = self.credentials.authorize(httplib2.Http())
        return self._http

    @property
    def service(self):
        '''
        Use apiclient's service discovery to get a drive api service object
        from which to make drive api calls.
        '''
        try: self._service
        except AttributeError: self._service = discovery.build('drive', 'v2', http=self.http)
        return self._service

    @property
    def credentials(self):
        '''
        Retrieve locally cached credentials if available, or request them from
        the server and store them locally.
        '''
        store = oauth2client.file.Storage(self.cached_credentials_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            if self.service_account_json_filename:
                with open(self.service_account_json_filename) as f:
                    account_info = json.load(f)
                email = account_info['client_email']
                key = account_info['private_key'].encode('utf8')
                credentials = client.SignedJwtAssertionCredentials(email, key, SCOPES)
                store.put(credentials)
            else:
                flow = client.flow_from_clientsecrets(self.client_secret_filename, self.scopes)
                flow.user_agent = self.name
                credentials = tools.run_flow(flow, store, self.flags)
        return credentials

    def get(self, id):
        '''
        Get a file by its globally unique id.
        '''
        try:
            return DriveObject(self, self.service.files().get(fileId=id).execute())
        except HttpError: pass

    def query(self, q, parent=None, maxResults=1000, limit=1000):
        '''
        Perform a query, optionally limited by a single parent and/or maxResults
        '''
        maxResults = min(maxResults, limit) # "limit" is more pythonic; accept either
        params = {
            'maxResults': maxResults,
            'orderBy': 'modifiedDate desc',
            'q': q,
        }
        if parent:
            params['folderId'] = parent.id if isinstance(parent, DriveObject) else parent
            filerefs = self.service.children().list(**params).execute()['items']
            files = [DriveObject(self, self.service.files().get(fileId=child['id']).execute()) for child in filerefs]
        else:
            files = [DriveObject(self, f) for f in self.service.files().list(**params).execute()['items']]
        if maxResults > 1:                  # Caller expects a list which can be empty
            return files
        return files[0] if files else None

    def file(self, name='', id=''):
        '''
        Get a single file by name or id
        '''
        q = 'title="{}" and mimeType!="{}" and trashed=false'.format(name, DriveObject.folder_type)
        return self.get(id) if id else self.query(q, maxResults=1)

    def folder(self, name='', id=''):
        '''
        Get a single folder by name or id
        '''
        q = 'title="{}" and mimeType="{}" and trashed=false'.format(name, DriveObject.folder_type)
        return self.get(id) if id else self.query(q, maxResults=1)


class DriveObject(object):
    '''
    Base class for all types of file-like objects. The primary purpose of this
    class is to wrap the json API response such that its contents are
    accessible as attributes.
    '''
    folder_type = 'application/vnd.google-apps.folder'

    def __new__(cls, client, attributes):
        # Act as a class factory and produce the appropriate subclass
        if not (client and attributes):
            return None
        new_cls = cls
        if cls is DriveObject:
            new_cls = DriveFolder if attributes['mimeType'] == DriveObject.folder_type else DriveFile
        return super(DriveObject, cls).__new__(new_cls)

    def __init__(self, client, attributes):
        self.client = client
        self.attributes = attributes

    def __getattr__(self, attr):
        return self.attributes.get(attr, '')

    def __repr__(self):
        return '<{} "{}">'.format(type(self).__name__, self.title)


class DriveFile(DriveObject):
    '''
    A file with methods for getting content in various forms
    '''
    def data_of_type(self, data_type=None, encoding=None):
        data = b''
        if self.exportLinks and data_type in self.exportLinks:
            data = self.client.http.request(self.exportLinks[data_type], 'GET')[1]
        elif self.downloadUrl:
            data = self.client.http.request(self.downloadUrl, 'GET')[1]
        return data.decode(encoding) if encoding else data

    def save_as(self, filename, replace=True):
        path = os.path.abspath(os.path.expanduser(filename))
        if not replace and os.path.exists(path):
            return
        with open(path, 'wb') as file:
            file.write(self.data)

    @property
    def data(self):
        return self.data_of_type()
    @property
    def text(self):
        return self.data_of_type('text/plain', 'utf-8-sig')
    @property
    def csv(self):
        return csv.reader(self.data_of_type('text/csv', 'utf-8-sig').splitlines())


class DriveFolder(DriveObject):
    '''
    A folder with methods for getting documents contained therein
    '''
    def files_of_type(self, mime_types=None):
        '''
        Get files by one or more mime_types
        '''
        q = 'mimeType != "{}"'.format(DriveObject.folder_type)
        if mime_types:
            if isinstance(mime_types, str):
                mime_types = [mime_types]
            q = '({})'.format(' or '.join('mimeType="{}"'.format(t) for t in mime_types))
        return self.client.query(q, parent=self)

    def file(self, name):
        '''
        Get a single child file by name
        '''
        q = 'mimeType != "{}" and title = "{}"'.format(DriveObject.folder_type, name)
        return self.client.query(q, parent=self, maxResults=1)

    def folder(self, name):
        '''
        Get a single child folder by name
        '''
        q = 'mimeType = "{}" and title = "{}"'.format(DriveObject.folder_type, name)
        return self.client.query(q, parent=self, maxResults=1)

    @property
    def files(self):
        return self.files_of_type()
    @property
    def folders(self):
        return self.files_of_type(DriveObject.folder_type)
    @property
    def documents(self):
        return self.files_of_type('application/vnd.google-apps.document')
    @property
    def spreadsheets(self):
        return self.files_of_type('application/vnd.google-apps.spreadsheet')
    @property
    def images(self):
        return self.files_of_type(['image/jpeg', 'image/png', 'image/gif', 'image/tiff', 'image/svg+xml'])




