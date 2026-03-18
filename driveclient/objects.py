"""
Drive object hierarchy: DriveObject → DriveFile, DriveFolder.

Wraps Drive API v3 file metadata and provides methods for content access,
uploading, and folder operations.
"""

import csv
import hashlib
import mimetypes
import os
from io import BytesIO

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from ._fields import (
    FILE_FIELDS, LIST_FIELDS, MIME_CSV, MIME_DOCUMENT, MIME_FOLDER, MIME_HTML,
    MIME_JPEG, MIME_GIF, MIME_PLAIN, MIME_PNG, MIME_SPREADSHEET, MIME_SVG,
    MIME_TIFF, IMAGE_TYPES, V2_TO_V3,
)
from .utils import debug_log, hashfile


class DriveObject:
    """
    Base class for all Drive objects. Acts as a factory in __new__ to produce
    DriveFile or DriveFolder based on mimeType.
    """
    folder_type = MIME_FOLDER

    def __new__(cls, client, attributes):
        if not (client and attributes):
            return None
        new_cls = cls
        if cls is DriveObject:
            new_cls = DriveFolder if attributes.get('mimeType') == MIME_FOLDER else DriveFile
        return super().__new__(new_cls)

    def __init__(self, client, attributes):
        self.client = client
        self.attributes = attributes

    def __getattr__(self, attr):
        # Support v2 attribute names via aliases
        mapped = V2_TO_V3.get(attr, attr)
        value = self.attributes.get(mapped)
        if value is not None:
            return value
        # Fall back to original attr name in case it exists directly
        if mapped != attr:
            value = self.attributes.get(attr)
            if value is not None:
                return value
        return ''

    def __repr__(self):
        return '<{} "{}">'.format(type(self).__name__, self.name)

    @property
    def id(self):
        return self.attributes.get('id', '')

    def as_document(self):
        """Bridge to Docs API — returns a Document for this file."""
        from .docs import Document
        return Document(self.client.docs, self.id)

    def as_spreadsheet(self):
        """Bridge to Sheets API — returns a Spreadsheet for this file."""
        from .sheets import Spreadsheet
        return Spreadsheet(self.client.sheets, self.id)


class DriveFile(DriveObject):
    """A file with methods for getting content in various forms."""

    def export(self, mime_type):
        """Export a Google Workspace file to the given MIME type. Returns bytes."""
        request = self.client.service.files().export(
            fileId=self.id, mimeType=mime_type
        )
        return self.client.execute(request)

    def download(self):
        """Download a non-Workspace file. Returns bytes."""
        request = self.client.service.files().get_media(fileId=self.id)
        buffer = BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buffer.getvalue()

    def data_of_type(self, data_type=None, encoding=None):
        """
        Get file content, either by export or download.
        Backward-compatible with v1 API.
        """
        if 'google-apps' in self.mimeType:
            if not data_type:
                # Default to PDF for Workspace files
                data_type = 'application/pdf'
            data = self.export(data_type)
        else:
            data = self.download()
        if isinstance(data, str):
            data = data.encode()
        return data.decode(encoding) if encoding else data

    def save_as(self, filename, replace=True):
        """Download this file and save it locally."""
        path = os.path.abspath(os.path.expanduser(filename))
        if os.path.exists(path):
            if not replace:
                debug_log('not replacing local file "{}"'.format(path))
                return
            if self.md5Checksum and self.md5Checksum == hashfile(path, hashlib.md5()):
                debug_log('not replacing local file "{}" with same hash'.format(path))
                return
        with open(path, 'wb') as f:
            data = self.data_of_type()
            if isinstance(data, str):
                data = data.encode()
            f.write(data)
        debug_log('saved local file "{}"'.format(path))
        return path

    def _write(self, **kw):
        drive_object = self.client.write(**kw)
        if drive_object:
            self.attributes = drive_object.attributes
            drive_object = self
        return drive_object

    def write(self, bytestring, mimetype, replace=True, convert=False):
        """Write a bytestring to this file."""
        return self._write(id=self.id, bytestring=bytestring,
                           mimetype=mimetype, replace=replace, convert=convert)

    def write_text(self, text, **kw):
        """Write text to this file, converting to a Google Doc if necessary."""
        return self._write(id=self.id, bytestring=text.encode(),
                           mimetype=MIME_PLAIN, **kw)

    def write_html(self, html, **kw):
        """Write HTML to this file, converting to a Google Doc if necessary."""
        return self._write(id=self.id,
                           bytestring=html.encode('ascii', 'xmlcharrefreplace'),
                           mimetype=MIME_HTML, **kw)

    def write_file(self, filename, mimetype=None, replace=True, convert=False):
        """Upload a file to replace this. Mimetype will be guessed if not supplied."""
        if not mimetype:
            mimetype = mimetypes.guess_type(filename)[0] or MIME_PLAIN
        with open(filename, 'rb') as f:
            return self._write(id=self.id, bytestring=f.read(),
                               mimetype=mimetype, replace=replace, convert=convert)

    @property
    def data(self):
        return self.data_of_type()

    @property
    def text(self):
        return self.data_of_type(MIME_PLAIN, 'utf-8-sig')

    @property
    def csv(self):
        return csv.reader(self.data_of_type(MIME_CSV, 'utf-8-sig').splitlines())


class DriveFolder(DriveObject):
    """A folder with methods for getting documents contained therein."""

    def files_of_type(self, mime_types=None):
        """Get files by one or more mime_types."""
        q = 'mimeType != "{}" and trashed=false'.format(MIME_FOLDER)
        if mime_types:
            if isinstance(mime_types, str):
                mime_types = [mime_types]
            q = '({}) and trashed=false'.format(
                ' or '.join('mimeType="{}"'.format(t) for t in mime_types)
            )
        return self.client.query(q, parent=self)

    def file(self, name):
        """Get a single child file by name."""
        q = 'mimeType != "{}" and name = "{}" and trashed=false'.format(MIME_FOLDER, name)
        return self.client.query(q, parent=self, page_size=1)

    def folder(self, name):
        """Get a single child folder by name."""
        q = 'mimeType = "{}" and name = "{}" and trashed=false'.format(MIME_FOLDER, name)
        return self.client.query(q, parent=self, page_size=1)

    def write(self, name, bytestring, mimetype, replace=True, convert=False):
        """Write a bytestring to this folder."""
        return self.client.write(name=name, folder=self, bytestring=bytestring,
                                 mimetype=mimetype, replace=replace, convert=convert)

    def write_text(self, name, text, **kw):
        """Write text to this folder, converting to a Google Doc."""
        return self.client.write(name=name, folder=self, bytestring=text.encode(),
                                 mimetype=MIME_PLAIN, **kw)

    def write_html(self, name, html, **kw):
        """Write HTML to this folder, converting to a Google Doc."""
        return self.client.write(name=name, folder=self,
                                 bytestring=html.encode('ascii', 'xmlcharrefreplace'),
                                 mimetype=MIME_HTML, **kw)

    def write_file(self, filename, mimetype=None, replace=True, convert=False):
        """Upload a file to this folder. Mimetype will be guessed if not supplied."""
        if not mimetype:
            mimetype = mimetypes.guess_type(filename)[0] or MIME_PLAIN
        with open(filename, 'rb') as f:
            return self.client.write(name=os.path.basename(filename), folder=self,
                                     bytestring=f.read(), mimetype=mimetype,
                                     replace=replace, convert=convert)

    @property
    def files(self):
        return self.files_of_type()

    @property
    def folders(self):
        return self.files_of_type(MIME_FOLDER)

    @property
    def documents(self):
        return self.files_of_type(MIME_DOCUMENT)

    @property
    def spreadsheets(self):
        return self.files_of_type(MIME_SPREADSHEET)

    @property
    def images(self):
        return self.files_of_type(IMAGE_TYPES)
