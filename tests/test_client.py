"""Tests for the DriveClient class."""

from unittest.mock import MagicMock, patch, call

import pytest

from driveclient import DriveClient, DriveFile, DriveFolder
from driveclient._fields import MIME_FOLDER, MIME_DOCUMENT, MIME_SPREADSHEET
from tests.conftest import make_file, make_folder, make_doc, make_spreadsheet_meta


class TestClientInit:

    @patch('driveclient.client.get_credentials')
    def test_lazy_credentials(self, mock_get_creds):
        c = DriveClient('test')
        mock_get_creds.assert_not_called()
        _ = c.credentials
        mock_get_creds.assert_called_once()

    @patch('driveclient.client.get_credentials')
    @patch('driveclient.client.discovery.build')
    def test_lazy_service(self, mock_build, mock_get_creds):
        c = DriveClient('test')
        mock_build.assert_not_called()
        _ = c.service
        mock_build.assert_called_once_with('drive', 'v3', credentials=c.credentials)


class TestGetFile:

    def test_get_by_id(self, client, mock_drive_service):
        file_data = make_file(id='abc123', name='report.pdf', mime='application/pdf')
        mock_drive_service.files().get().execute.return_value = file_data

        result = client.get('abc123')

        assert isinstance(result, DriveFile)
        assert result.id == 'abc123'
        assert result.name == 'report.pdf'

    def test_get_returns_none_for_missing(self, client, mock_drive_service):
        mock_drive_service.files().get().execute.return_value = None

        with patch('driveclient.client.execute_with_backoff', return_value=None):
            result = client.get('nonexistent')

        assert result is None


class TestFindFile:

    def test_file_by_name(self, client):
        file_data = make_file(name='notes.txt')
        with patch.object(client, 'query', return_value=DriveFile(client, file_data)) as mock_q:
            result = client.file(name='notes.txt')

            mock_q.assert_called_once()
            args = mock_q.call_args
            assert 'name="notes.txt"' in args[0][0]
            assert MIME_FOLDER in args[0][0]  # excludes folders
            assert args[1]['page_size'] == 1

    def test_file_by_id_delegates_to_get(self, client):
        file_data = make_file(id='xyz')
        with patch.object(client, 'get', return_value=DriveFile(client, file_data)) as mock_get:
            result = client.file(id='xyz')
            mock_get.assert_called_once_with('xyz')


class TestFindFolder:

    def test_folder_by_name(self, client):
        folder_data = make_folder(name='Photos')
        with patch.object(client, 'query', return_value=DriveFolder(client, folder_data)) as mock_q:
            result = client.folder(name='Photos')

            args = mock_q.call_args
            assert 'name="Photos"' in args[0][0]
            assert f'mimeType="{MIME_FOLDER}"' in args[0][0]
            assert args[1]['page_size'] == 1

    def test_folder_by_id_delegates_to_get(self, client):
        with patch.object(client, 'get') as mock_get:
            client.folder(id='fld1')
            mock_get.assert_called_once_with('fld1')


class TestQuery:

    def test_basic_query(self, client):
        file_data = [make_file(id=f'f{i}', name=f'file{i}.txt') for i in range(3)]
        with patch('driveclient.client.execute_with_backoff') as mock_exec:
            mock_exec.return_value = {'files': file_data}
            results = client.query('name contains "file"')

        assert len(results) == 3
        assert all(isinstance(r, DriveFile) for r in results)

    def test_query_with_parent(self, client):
        folder = DriveFolder(client, make_folder(id='parent_id'))
        with patch('driveclient.client.execute_with_backoff') as mock_exec:
            mock_exec.return_value = {'files': []}
            client.query('trashed=false', parent=folder)

            call_args = mock_exec.call_args
            # The service.files().list() should have been called with parent in q
            list_call = mock_drive_service = client.service.files().list
            q_arg = list_call.call_args[1]['q']
            assert '"parent_id" in parents' in q_arg

    def test_query_page_size_1_returns_single(self, client):
        with patch('driveclient.client.execute_with_backoff') as mock_exec:
            mock_exec.return_value = {'files': [make_file()]}
            result = client.query('name="x"', page_size=1)

        assert isinstance(result, DriveFile)

    def test_query_page_size_1_returns_none_if_empty(self, client):
        with patch('driveclient.client.execute_with_backoff') as mock_exec:
            mock_exec.return_value = {'files': []}
            result = client.query('name="x"', page_size=1)

        assert result is None

    def test_query_pagination(self, client):
        page1 = {'files': [make_file(id='f1')], 'nextPageToken': 'token2'}
        page2 = {'files': [make_file(id='f2')]}
        with patch('driveclient.client.execute_with_backoff', side_effect=[page1, page2]):
            results = client.query('trashed=false', page_size=5000)

        assert len(results) == 2


class TestRoot:

    def test_root_returns_folder(self, client):
        with patch('driveclient.client.execute_with_backoff', return_value=make_folder(id='root_id')):
            root = client.root

        assert isinstance(root, DriveFolder)
        assert root.id == 'root_id'


class TestCopy:

    def test_copy_file(self, client):
        with patch('driveclient.client.execute_with_backoff') as mock_exec:
            mock_exec.return_value = make_file(id='copy1', name='Copy of test.txt')
            result = client.copy('orig1', name='Copy of test.txt', parent_id='folder1')

        assert isinstance(result, DriveFile)
        assert result.name == 'Copy of test.txt'
        copy_call = client.service.files().copy
        body = copy_call.call_args[1]['body']
        assert body['name'] == 'Copy of test.txt'
        assert body['parents'] == ['folder1']


class TestWriteFile:

    def test_create_new_file_in_folder(self, client):
        folder = DriveFolder(client, make_folder(id='fld1'))
        # folder.file() returns None (no existing file)
        with patch.object(folder, 'file', return_value=None), \
             patch('driveclient.client.execute_with_backoff') as mock_exec:
            # First call is for folder.file (returns None via patch above)
            # Root call returns a root folder, create returns new file
            mock_exec.return_value = make_file(id='new1', name='new.txt')
            result = client.write(name='new.txt', folder=folder,
                                  bytestring=b'hello', mimetype='text/plain',
                                  convert=False)

        assert result is not None
        assert result.name == 'new.txt'

    def test_write_with_convert_to_doc(self, client):
        folder = DriveFolder(client, make_folder(id='fld1'))
        with patch.object(folder, 'file', return_value=None), \
             patch('driveclient.client.execute_with_backoff') as mock_exec:
            mock_exec.return_value = make_doc(id='doc1', name='My Doc')
            result = client.write(name='My Doc', folder=folder,
                                  bytestring=b'<h1>Hello</h1>',
                                  mimetype='text/html', convert=True)

        assert result is not None
        create_call = client.service.files().create
        body = create_call.call_args[1]['body']
        assert body['mimeType'] == MIME_DOCUMENT

    def test_write_with_convert_csv_to_spreadsheet(self, client):
        folder = DriveFolder(client, make_folder(id='fld1'))
        with patch.object(folder, 'file', return_value=None), \
             patch('driveclient.client.execute_with_backoff') as mock_exec:
            mock_exec.return_value = make_spreadsheet_meta(id='ss1')
            result = client.write(name='data.csv', folder=folder,
                                  bytestring=b'a,b\n1,2',
                                  mimetype='text/csv', convert=True)

        create_call = client.service.files().create
        body = create_call.call_args[1]['body']
        assert body['mimeType'] == MIME_SPREADSHEET

    def test_update_existing_file(self, client):
        existing = DriveFile(client, make_file(id='exist1', name='data.txt'))
        with patch.object(client, 'file', return_value=existing), \
             patch('driveclient.client.execute_with_backoff') as mock_exec:
            mock_exec.return_value = make_file(id='exist1', name='data.txt')
            result = client.write(id='exist1', bytestring=b'updated', mimetype='text/plain',
                                  convert=False)

        assert result is not None
        client.service.files().update.assert_called()

    def test_write_no_replace_skips(self, client):
        existing = DriveFile(client, make_file(id='exist1'))
        folder = DriveFolder(client, make_folder(id='fld1'))
        with patch.object(folder, 'file', return_value=existing):
            result = client.write(name='test.txt', folder=folder,
                                  bytestring=b'data', mimetype='text/plain',
                                  replace=False, convert=False)

        assert result is None

    def test_write_returns_none_without_name_or_id(self, client):
        result = client.write(bytestring=b'data', mimetype='text/plain', convert=False)
        assert result is None
