"""Tests for DriveObject, DriveFile, and DriveFolder."""

import hashlib
import os
from io import BytesIO
from unittest.mock import MagicMock, patch, mock_open

import pytest

from driveclient import DriveFile, DriveFolder, DriveObject
from driveclient._fields import (
    MIME_FOLDER, MIME_DOCUMENT, MIME_SPREADSHEET, MIME_PLAIN, MIME_CSV, MIME_HTML,
)
from tests.conftest import make_file, make_folder, make_doc, make_spreadsheet_meta


class TestDriveObjectFactory:

    def test_file_mime_creates_drivefile(self):
        client = MagicMock()
        obj = DriveObject(client, make_file(mime='text/plain'))
        assert isinstance(obj, DriveFile)

    def test_folder_mime_creates_drivefolder(self):
        client = MagicMock()
        obj = DriveObject(client, make_folder())
        assert isinstance(obj, DriveFolder)

    def test_doc_mime_creates_drivefile(self):
        client = MagicMock()
        obj = DriveObject(client, make_doc())
        assert isinstance(obj, DriveFile)

    def test_spreadsheet_mime_creates_drivefile(self):
        client = MagicMock()
        obj = DriveObject(client, make_spreadsheet_meta())
        assert isinstance(obj, DriveFile)

    def test_returns_none_for_empty_attributes(self):
        client = MagicMock()
        obj = DriveObject(client, {})
        assert obj is None

    def test_returns_none_for_no_client(self):
        obj = DriveObject(None, make_file())
        assert obj is None


class TestDriveObjectAttributes:

    def test_id(self):
        client = MagicMock()
        obj = DriveObject(client, make_file(id='xyz'))
        assert obj.id == 'xyz'

    def test_name(self):
        client = MagicMock()
        obj = DriveObject(client, make_file(name='hello.txt'))
        assert obj.name == 'hello.txt'

    def test_v2_alias_title(self):
        client = MagicMock()
        obj = DriveObject(client, make_file(name='doc.txt'))
        assert obj.title == 'doc.txt'

    def test_v2_alias_modifiedDate(self):
        client = MagicMock()
        obj = DriveObject(client, make_file())
        assert obj.modifiedDate == obj.modifiedTime

    def test_missing_attr_returns_empty_string(self):
        client = MagicMock()
        obj = DriveObject(client, make_file())
        assert obj.nonexistent_attr == ''

    def test_repr(self):
        client = MagicMock()
        f = DriveObject(client, make_file(name='test.txt'))
        assert repr(f) == '<DriveFile "test.txt">'
        d = DriveObject(client, make_folder(name='My Folder'))
        assert repr(d) == '<DriveFolder "My Folder">'


class TestDriveFileExport:

    def test_export_workspace_file(self):
        client = MagicMock()
        f = DriveFile(client, make_doc(id='doc1'))
        client.execute.return_value = b'exported content'

        result = f.export('application/pdf')

        client.service.files().export.assert_called_once_with(
            fileId='doc1', mimeType='application/pdf'
        )
        assert result == b'exported content'

    def test_download_binary_file(self):
        client = MagicMock()
        f = DriveFile(client, make_file(id='bin1', mime='application/pdf'))

        # Mock MediaIoBaseDownload behavior
        with patch('driveclient.objects.MediaIoBaseDownload') as mock_dl_cls:
            mock_dl = MagicMock()
            mock_dl.next_chunk.side_effect = [(None, False), (None, True)]
            mock_dl_cls.return_value = mock_dl

            f.download()

        client.service.files().get_media.assert_called_once_with(fileId='bin1')


class TestDriveFileDataOfType:

    def test_workspace_file_exports_pdf_by_default(self):
        client = MagicMock()
        f = DriveFile(client, make_doc())
        client.execute.return_value = b'pdf bytes'

        result = f.data_of_type()

        client.service.files().export.assert_called_once_with(
            fileId='doc1', mimeType='application/pdf'
        )

    def test_workspace_file_exports_requested_type(self):
        client = MagicMock()
        f = DriveFile(client, make_doc())
        client.execute.return_value = b'plain text'

        result = f.data_of_type(data_type=MIME_PLAIN)

        client.service.files().export.assert_called_once_with(
            fileId='doc1', mimeType=MIME_PLAIN
        )

    def test_encoding_decodes_bytes(self):
        client = MagicMock()
        f = DriveFile(client, make_doc())
        client.execute.return_value = b'hello'

        result = f.data_of_type(data_type=MIME_PLAIN, encoding='utf-8')
        assert result == 'hello'

    def test_text_property(self):
        client = MagicMock()
        f = DriveFile(client, make_doc())
        # BOM-prefixed content (utf-8-sig strips BOM)
        client.execute.return_value = b'\xef\xbb\xbfhello'

        assert f.text == 'hello'


class TestDriveFileSaveAs:

    def test_save_new_file(self, tmp_path):
        client = MagicMock()
        f = DriveFile(client, make_file(id='f1', mime='application/pdf'))
        client.execute.return_value = b'pdf bytes'

        with patch('driveclient.objects.MediaIoBaseDownload') as mock_dl_cls:
            mock_dl = MagicMock()
            mock_dl.next_chunk.return_value = (None, True)
            mock_dl_cls.return_value = mock_dl

            dest = str(tmp_path / 'output.pdf')
            result = f.save_as(dest)

        assert result == dest
        assert os.path.exists(dest)

    def test_save_skips_when_replace_false_and_exists(self, tmp_path):
        client = MagicMock()
        f = DriveFile(client, make_file())

        dest = str(tmp_path / 'existing.txt')
        with open(dest, 'w') as fh:
            fh.write('existing')

        result = f.save_as(dest, replace=False)
        assert result is None

    def test_save_skips_when_hash_matches(self, tmp_path):
        content = b'file content'
        md5 = hashlib.md5(content).hexdigest()
        client = MagicMock()
        f = DriveFile(client, make_file(md5Checksum=md5))

        dest = str(tmp_path / 'same.txt')
        with open(dest, 'wb') as fh:
            fh.write(content)

        result = f.save_as(dest)
        assert result is None  # skipped because hash matches


class TestDriveFileWrite:

    def test_write_text(self):
        client = MagicMock()
        f = DriveFile(client, make_file(id='f1'))
        client.write.return_value = DriveFile(client, make_file(id='f1'))

        result = f.write_text('hello world')

        client.write.assert_called_once()
        kwargs = client.write.call_args[1]
        assert kwargs['bytestring'] == b'hello world'
        assert kwargs['mimetype'] == MIME_PLAIN
        assert kwargs['id'] == 'f1'

    def test_write_html(self):
        client = MagicMock()
        f = DriveFile(client, make_file(id='f1'))
        client.write.return_value = DriveFile(client, make_file(id='f1'))

        result = f.write_html('<p>hello</p>')

        kwargs = client.write.call_args[1]
        assert kwargs['mimetype'] == MIME_HTML
        assert kwargs['id'] == 'f1'


class TestDriveFolderQuery:

    def test_file_by_name(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder(id='fld1'))
        client.query.return_value = DriveFile(client, make_file(name='report.txt'))

        result = folder.file('report.txt')

        client.query.assert_called_once()
        q = client.query.call_args[0][0]
        assert 'name = "report.txt"' in q
        assert MIME_FOLDER in q  # excludes folders
        assert client.query.call_args[1]['parent'] is folder
        assert client.query.call_args[1]['page_size'] == 1

    def test_folder_by_name(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder(id='fld1'))
        child = DriveFolder(client, make_folder(id='fld2', name='Subfolder'))
        client.query.return_value = child

        result = folder.folder('Subfolder')

        q = client.query.call_args[0][0]
        assert f'mimeType = "{MIME_FOLDER}"' in q
        assert 'name = "Subfolder"' in q

    def test_files_property(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder())
        files = [DriveFile(client, make_file(id=f'f{i}')) for i in range(3)]
        client.query.return_value = files

        result = folder.files
        assert len(result) == 3

    def test_folders_property(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder())
        client.query.return_value = []

        result = folder.folders

        q = client.query.call_args[0][0]
        assert MIME_FOLDER in q

    def test_documents_property(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder())
        client.query.return_value = []

        result = folder.documents

        q = client.query.call_args[0][0]
        assert MIME_DOCUMENT in q

    def test_spreadsheets_property(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder())
        client.query.return_value = []

        result = folder.spreadsheets

        q = client.query.call_args[0][0]
        assert MIME_SPREADSHEET in q


class TestDriveFolderWrite:

    def test_write_text_to_folder(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder(id='fld1'))
        client.write.return_value = DriveFile(client, make_file(name='note.txt'))

        result = folder.write_text('note.txt', 'hello')

        client.write.assert_called_once()
        kwargs = client.write.call_args[1]
        assert kwargs['name'] == 'note.txt'
        assert kwargs['folder'] is folder
        assert kwargs['bytestring'] == b'hello'
        assert kwargs['mimetype'] == MIME_PLAIN

    def test_write_html_to_folder(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder(id='fld1'))
        client.write.return_value = DriveFile(client, make_doc())

        result = folder.write_html('page.html', '<h1>Hi</h1>')

        kwargs = client.write.call_args[1]
        assert kwargs['mimetype'] == MIME_HTML

    def test_write_bytes_to_folder(self):
        client = MagicMock()
        folder = DriveFolder(client, make_folder(id='fld1'))
        client.write.return_value = DriveFile(client, make_file())

        folder.write('data.bin', b'\x00\x01', 'application/octet-stream')

        kwargs = client.write.call_args[1]
        assert kwargs['bytestring'] == b'\x00\x01'
        assert kwargs['mimetype'] == 'application/octet-stream'

    def test_write_file_from_disk(self, tmp_path):
        client = MagicMock()
        folder = DriveFolder(client, make_folder(id='fld1'))
        client.write.return_value = DriveFile(client, make_file())

        path = tmp_path / 'upload.txt'
        path.write_text('file content')

        folder.write_file(str(path))

        kwargs = client.write.call_args[1]
        assert kwargs['bytestring'] == b'file content'
        assert kwargs['name'] == 'upload.txt'
