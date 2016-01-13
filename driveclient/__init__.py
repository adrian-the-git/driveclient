"""
Abstracts away much of what's needed for read-only access to Google
Drive via its API, simplifying the common case of reading data from
documents, spreadsheets, and downloading images.

DriveClient instances contain a service property which can be used
to access the full API as the authenticated user. In order to make
changes, set an appropriate read/write scope when instantiating a
client.
"""

import argparse
import csv
import os
import json
from functools import partial

import httplib2
import oauth2client
from apiclient import discovery
from oauth2client import client, tools


CLIENT_SECRET_FILENAME = 'client_secret.json'
CACHED_CREDENTIALS_FILENAME = 'drive_client.json'
SCOPES = 'https://www.googleapis.com/auth/drive.readonly'


class DriveClient(object):
    '''
    This object handles the connection to Google's Drive API
    and provides a way to get a folder by name. Further file
    operations can be performed by calling methods on that
    folder object (an instance of DriveFolder)
    '''
    def __init__(self, name, client_secret_filename=CLIENT_SECRET_FILENAME, 
                 cached_credentials_filename=CACHED_CREDENTIALS_FILENAME, 
                 scopes=SCOPES, service_account_json_filename=None):
        '''
        If a service_account_json_filename is provided, a
        private key will be used instead of the user-assisted
        OAuth flow which requires a browser.
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
        self.flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
        
    @property
    def http(self):
        '''
        This process is handled entirely by the credentials
        object from the oauth2client library.
        '''
        try: self._http
        except AttributeError: self._http = self.credentials.authorize(httplib2.Http())
        return self._http

    @property
    def service(self):
        '''
        Use apiclient's service discovery to get a drive api
        service object from which to make drive api calls.
        '''
        try: self._service
        except AttributeError: self._service = discovery.build('drive', 'v2', http=self.http)
        return self._service
        
    @property
    def credentials(self):
        '''
        Retrieve locally cached credentials if available, or 
        request them from the server and store them locally.
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

    def folder(self, name): 
        '''
        The basic method of the drive client object. We're very 
        old-fashioned in that all activities basically revolve
        around first getting a folder object from which children
        may be queried
        '''
        folders = self.folders(name, 1)
        return folders[0] if folders else []

    def folders(self, name, limit=1000):
        params = {
            'maxResults': limit,
            'orderBy': 'modifiedDate desc',      
            'q': 'title="{}" and mimeType="application/vnd.google-apps.folder" and trashed=false'.format(name),
        }
        return [DriveFolder(self, folder) 
            for folder in self.service.files().list(**params).execute().get('items', [])[:limit]]


class DriveObject(object):
    '''
    Base class for all types of file-like objects.
    The primary purpose of this class is to wrap
    the json API response such that its contents
    are accessible as attributes.
    '''
    def __init__(self, client, attributes):
        self.client = client
        self.attributes = attributes

    def __getattr__(self, attr):
        return self.attributes.get(attr, '')

    def __repr__(self):
        return '<{} "{}">'.format(type(self).__name__, self.title)

        
class DriveFile(DriveObject):
    '''
    A file of indeterminate type
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
    A folder of type application/vnd.google-apps.folder
    '''
    def files_of_type(self, mime_types=None):
        folder_type = 'application/vnd.google-apps.folder'
        params = {
            'folderId': self.id, 
            'maxResults': 1000,
            'orderBy': 'modifiedDate desc', 
            'q': 'mimeType != "{}"'.format(folder_type),
        }
        if mime_types:
            if isinstance(mime_types, str):
                mime_types = [mime_types]
            params['q'] = '({})'.format(' or '.join('mimeType="{}"'.format(t) for t in mime_types))
        children = self.client.service.children().list(**params).execute().get('items', [])
        children = [self.client.service.files().get(fileId=child['id']).execute() for child in children]
        return [(DriveFolder if child['mimeType'] == folder_type else DriveFile)(self.client, child)
            for child in children]

    @property
    def files(self):
        return self.files_of_type()
    @property
    def folders(self):
        return self.files_of_type('application/vnd.google-apps.folder')
    @property
    def documents(self):
        return self.files_of_type('application/vnd.google-apps.document')
    @property
    def spreadsheets(self):
        return self.files_of_type('application/vnd.google-apps.spreadsheet')
    @property
    def images(self):
        return self.files_of_type(['image/jpeg', 'image/png', 'image/gif', 'image/tiff', 'image/svg+xml'])




