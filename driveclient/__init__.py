#!/bin/cat

"""
Abstracts away much of what's needed for using Google Drive via its API, 
simplifying the common case of reading data from documents, spreadsheets, 
and downloading images. Basic file uploading is supported.

DriveClient instances contain a service property which can be used to access
the full v2 API as an authenticated user. Be aware that many functions return
None upon failure (rather than raising exceptions) so you should always check
for truthy results before proceeding.
"""

import argparse
import csv
import os
import hashlib
import json
import mimetypes
import random
import time
from functools import partial
from io import BytesIO
from pprint import pprint
from urllib.parse import parse_qs, urlparse

import httplib2
import oauth2client
from apiclient import discovery
from apiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
from oauth2client import client, tools
from oauth2client.service_account import ServiceAccountCredentials


CLIENT_SECRET_FILENAME = 'client_secret.json'
CACHED_CREDENTIALS_DIRECTORY = '~/.credentials'
SCOPES = 'https://www.googleapis.com/auth/drive'
DEBUG = 'DRIVECLIENT_DEBUG' in os.environ


class DriveClient(object):
    '''
    This object handles the connection to Google's Drive API and provides basic
    methods to fetch files or folders by id or a custom query. Shortcuts are
    provided for the common case of fetching a file or folder by name or id.
    '''
    def __init__(self, name, client_secret_filename=CLIENT_SECRET_FILENAME,
                 cached_credentials_directory=CACHED_CREDENTIALS_DIRECTORY,
                 scopes=SCOPES, service_account_json_filename=None):
        '''
        If a service_account_json_filename is provided, a private key will be
        used instead of the user-assisted OAuth flow which requires a browser.
        '''
        self.name = name
        self.client_secret_filename = client_secret_filename

        cached_credentials_directory = os.path.expanduser(cached_credentials_directory)
        if not os.path.exists(cached_credentials_directory):
            os.makedirs(cached_credentials_directory)
        self.cached_credentials_filename = os.path.join(cached_credentials_directory, name + '.json')

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
        store = oauth2client.file.Storage(self.cached_credentials_filename)
        credentials = store.get()
        if not credentials or credentials.invalid:
            if self.service_account_json_filename:
                credentials = ServiceAccountCredentials.from_json_keyfile_name(self.service_account_json_filename, scopes=self.scopes)
                store.put(credentials)
            else:
                flow = client.flow_from_clientsecrets(self.client_secret_filename, self.scopes)
                flow.user_agent = self.name
                credentials = tools.run_flow(flow, store, self.flags)
        return credentials

    def execute(self, request):
        '''
        Execute a request with simple exponential backoff
        '''
        DEBUG and dump_request(request)

        for i in range(5):
            try:
                return request.execute()
            except HttpError as error:
                reason = error._get_reason().lower().replace(' ','')
                if 'ratelimitexceeded' in reason:
                    time.sleep(2**i + random.random())
                    continue
                elif 'notfound' in reason:
                    return
                elif 'invalidchange' in reason:
                    return
                raise

    @property
    def root(self):
        '''
        Return the root folder
        '''
        about = self.execute(self.service.about().get())
        if about:
            return self.folder(id=about['rootFolderId'])

    def get(self, id):
        '''
        Get a file by its globally unique id.
        '''
        result = self.execute(self.service.files().get(fileId=id))
        if result:
            return DriveObject(self, result)

    def get_change(self, changeId):
        '''
        Get a file by its ephemeral change id.
        '''
        result = self.execute(self.service.changes().get(changeId=changeId))
        if result:
            return DriveObject(self, result['file'])

    def query(self, q, parent=None, maxResults=1000):
        '''
        Perform a query, optionally limited by a single parent and/or maxResults.
        '''
        if parent:
            q = '"{}" in parents and ({})'.format(parent.id if isinstance(parent, DriveObject) else parent, q)
        params = {
            'maxResults': maxResults,
            'orderBy': 'modifiedDate desc',
            'q': q,
        }
        files = [DriveObject(self, f) for f in self.execute(self.service.files().list(**params))['items']]
        if maxResults > 1:                  # Caller expects a list which can be empty
            return files
        return files[0] if files else None

    def file(self, name='', id=''):
        '''
        Get a single file by name or id.
        '''
        q = 'title="{}" and mimeType!="{}" and trashed=false'.format(name, DriveObject.folder_type)
        return self.get(id) if id else self.query(q, maxResults=1)

    def folder(self, name='', id=''):
        '''
        Get a single folder by name or id.
        '''
        q = 'title="{}" and mimeType="{}" and trashed=false'.format(name, DriveObject.folder_type)
        return self.get(id) if id else self.query(q, maxResults=1)

    def write(self, name='', folder=None, bytestring=b'', mimetype='text/plain', replace=True, convert=True, id=''):
        '''
        Write file data (given as bytes). Specify either a filename and folder
        OR a file id. Folder defaults to root when not specified with filename.

        Despite the apparent simplicity of this function, the semantics of
        uploading and converting files are fairly subtle and complicated. For
        example, files can only be updated if their mime-type remains unchanged,
        so we must first delete the file and re-insert it. This will fail if
        the file is owned by another account.

        There are probably corner cases where the convert flag will not work,
        so set the DRIVECLIENT_DEBUG environment variable if you run into
        problems.
        '''
        if id:
            existing_file = self.file(id=id)
            if not existing_file:
                return
            name = existing_file.title
            parents = existing_file.parents
        elif name:
            if isinstance(folder, str):
                folder = self.folder(name=folder)
                if not folder:
                    return
            elif not folder:
                folder = self.root
            existing_file = folder.file(name)
            parents = [{'id': folder.id}]
        else: return

        params = {
            'body': {
                'title': name,
                'parents': parents
            },
            'convert': convert,
            'media_body': MediaIoBaseUpload(BytesIO(bytestring), mimetype=mimetype),
        }

        if existing_file and not replace:
            DEBUG and print('driveclient: not replacing "{}"'.format(name))
            return
        if existing_file:
            if ((not convert and 'google-apps' in existing_file.mimeType) or
                (convert and not 'google-apps' in existing_file.mimeType)):
                try:
                    DEBUG and print('driveclient: deleting "{}" for type conversion'.format(name))
                    self.execute(self.service.files().delete(fileId=existing_file.id))
                except HttpError:
                    DEBUG and print('driveclient: can\'t replace "{}" for type conversion'.format(name))
                    return
            else:
                DEBUG and print('driveclient: updating "{}"'.format(name))
                return DriveObject(self, self.execute(self.service.files().update(fileId=existing_file.id, **params)))
        DEBUG and print('driveclient: inserting "{}"'.format(name))
        return DriveObject(self, self.execute(self.service.files().insert(**params)))


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
        if self.exportLinks:
            if data_type not in self.exportLinks:
                # Pick the opendocument format
                data_type = next(t for t in self.exportLinks if 'opendocument' in t)
            data = self.client.http.request(self.exportLinks[data_type], 'GET')[1]
        elif self.downloadUrl:
            data = self.client.http.request(self.downloadUrl, 'GET')[1]
        return data.decode(encoding) if encoding else data

    def save_as(self, filename, replace=True):
        path = os.path.abspath(os.path.expanduser(filename))
        if os.path.exists(path):
            if not replace:
                DEBUG and print('driveclient: not replacing local file "{}"'.format(path))
                return
            if self.md5Checksum and self.md5Checksum == hashfile(path, hashlib.md5()):
                DEBUG and print('driveclient: not replacing local file "{}" with same hash'.format(path))
                return
        with open(path, 'wb') as file:
            file.write(self.data)
        DEBUG and print('driveclient: saved local file "{}"'.format(path))

    def _write(self, **kw):
        drive_object = self.client.write(**kw)
        if drive_object:
            self.attributes = drive_object.attributes
            drive_object = self
        return drive_object

    def write(self, bytestring, mimetype, replace=True, convert=False):
        '''
        Write a bytestring to this file. A mimetype is required.
        '''
        return self._write(id=self.id, bytestring=bytestring,
            mimetype=mimetype, replace=replace, convert=convert)

    def write_text(self, text, **kw):
        '''
        Write text to this file, converting to a google doc if necessary
        '''
        return self._write(id=self.id, bytestring=text.encode(),
            mimetype='text/plain', **kw)

    def write_html(self, html, **kw):
        '''
        Write html to this file, converting to a google doc if necessary
        '''
        return self._write(id=self.id, bytestring=html.encode('ascii', 'xmlcharrefreplace'),
            mimetype='text/html', **kw)

    def write_file(self, filename, mimetype=None, replace=True, convert=False):
        '''
        Upload a file to replace this. Mimetype will be guessed if not supplied.
        '''
        if not mimetype:
            mimetype = mimetypes.guess_type(filename)[0] or 'text/plain'
        with open(filename, 'rb') as f:
            return self._write(id=self.id, bytestring=f.read(),
                mimetype=mimetype, replace=replace, convert=convert)

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
        q = 'mimeType != "{}" and trashed=false'.format(DriveObject.folder_type)
        if mime_types:
            if isinstance(mime_types, str):
                mime_types = [mime_types]
            q = '({}) and trashed=false'.format(' or '.join('mimeType="{}"'.format(t) for t in mime_types))
        return self.client.query(q, parent=self)

    def file(self, name):
        '''
        Get a single child file by name
        '''
        q = 'mimeType != "{}" and title = "{}" and trashed=false'.format(DriveObject.folder_type, name)
        return self.client.query(q, parent=self, maxResults=1)

    def folder(self, name):
        '''
        Get a single child folder by name
        '''
        q = 'mimeType = "{}" and title = "{}" and trashed=false'.format(DriveObject.folder_type, name)
        return self.client.query(q, parent=self, maxResults=1)

    def write(self, name, bytestring, mimetype, replace=True, convert=False):
        '''
        Write a bytestring to this folder. A mimetype is required.
        '''
        return self.client.write(name=name, folder=self, bytestring=bytestring,
            mimetype=mimetype, replace=replace, convert=convert)

    def write_text(self, name, text, **kw):
        '''
        Write text to this folder, converting to a google doc
        '''
        return self.client.write(name=name, folder=self, bytestring=text.encode(),
            mimetype='text/plain', **kw)

    def write_html(self, name, html, **kw):
        '''
        Write html to this folder, converting to a google doc
        '''
        return self.client.write(name=name, folder=self, bytestring=html.encode('ascii', 'xmlcharrefreplace'),
            mimetype='text/html', **kw)

    def write_file(self, filename, mimetype=None, replace=True, convert=False):
        '''
        Upload a file to this folder. Mimetype will be guessed if not supplied.
        '''
        if not mimetype:
            mimetype = mimetypes.guess_type(filename)[0] or 'text/plain'
        with open(filename, 'rb') as f:
            return self.client.write(name=os.path.basename(filename), folder=self, bytestring=f.read(),
                mimetype=mimetype, replace=replace, convert=convert)

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


def dump_request(request):
    '''
    Print some noisy but useful information about a request.
    '''
    print('driveclient:', request.methodId)
    print(request.method, request.uri)
    if request.method == 'GET':
        pprint(parse_qs(urlparse(request.uri).query))
    elif request.method in ('PUT', 'POST'):
        pprint(request.body)
    print()


def hashfile(filename, hasher=None, blocksize=2**16):
    '''
    Hash a file without reading the entire thing into memory
    '''
    hasher = hasher or hashlib.sha1()
    with open(filename, 'rb') as f:
        [hasher.update(block) for block in iter(partial(f.read, blocksize), b'')]
        return hasher.hexdigest()


