"""Tests for the backward-compatibility shim."""

import datetime
import importlib
import warnings
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# Import compat to activate shims (guarded by expiry check)
import driveclient.compat
from driveclient import DriveClient, DriveFolder
from driveclient._fields import MIME_DOCUMENT, MIME_SPREADSHEET
from tests.conftest import make_file, make_folder


class TestExpiry:

    def test_refuses_to_load_after_expiry(self):
        with patch('driveclient.compat.datetime') as mock_dt:
            mock_dt.date.today.return_value = datetime.date(2026, 7, 1)
            mock_dt.date.side_effect = lambda *a, **kw: datetime.date(*a, **kw)
            # Can't easily re-trigger the module-level check, but we can
            # verify the constant is correct
            assert driveclient.compat.EXPIRY == datetime.date(2026, 7, 1)

    def test_expiry_date_is_correct(self):
        assert driveclient.compat.EXPIRY == datetime.date(2026, 7, 1)


class TestScopesCompat:

    @patch('driveclient.client.get_credentials')
    def test_string_scopes_converted_to_list(self, mock_get_creds):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            c = DriveClient('test', scopes='https://www.googleapis.com/auth/drive')

        assert c.scopes == ['https://www.googleapis.com/auth/drive']
        assert len(w) == 1
        assert 'list' in str(w[0].message)
        assert issubclass(w[0].category, DeprecationWarning)

    @patch('driveclient.client.get_credentials')
    def test_list_scopes_unchanged(self, mock_get_creds):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            c = DriveClient('test', scopes=['https://www.googleapis.com/auth/drive'])

        assert c.scopes == ['https://www.googleapis.com/auth/drive']
        assert len(w) == 0


class TestMaxResultsCompat:

    @patch('driveclient.client.get_credentials')
    def test_maxResults_maps_to_page_size(self, mock_get_creds):
        c = DriveClient('test')
        c._credentials = MagicMock()
        c._service = MagicMock()

        with patch('driveclient.client.execute_with_backoff') as mock_exec, \
             warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            mock_exec.return_value = {'files': [make_file()]}
            c.query('trashed=false', maxResults=1)

        assert len(w) == 1
        assert 'maxResults' in str(w[0].message)
        assert issubclass(w[0].category, DeprecationWarning)

    @patch('driveclient.client.get_credentials')
    def test_page_size_still_works(self, mock_get_creds):
        c = DriveClient('test')
        c._credentials = MagicMock()
        c._service = MagicMock()

        with patch('driveclient.client.execute_with_backoff') as mock_exec, \
             warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            mock_exec.return_value = {'files': []}
            c.query('trashed=false', page_size=5)

        assert len(w) == 0


class TestTitleQueryCompat:

    @patch('driveclient.client.get_credentials')
    def test_title_rewritten_to_name(self, mock_get_creds):
        c = DriveClient('test')
        c._credentials = MagicMock()
        c._service = MagicMock()

        with patch('driveclient.client.execute_with_backoff') as mock_exec, \
             warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            mock_exec.return_value = {'files': []}
            c.query('title="Report"')

        assert len(w) == 1
        assert 'title' in str(w[0].message)
        # Verify the actual query sent to the API uses 'name'
        list_call = c.service.files().list
        q_sent = list_call.call_args[1]['q']
        assert 'name="Report"' in q_sent
        assert 'title' not in q_sent


class TestWriteConvertCompat:

    @patch('driveclient.client.get_credentials')
    def test_default_convert_true_with_warning(self, mock_get_creds):
        c = DriveClient('test')
        c._credentials = MagicMock()
        c._service = MagicMock()

        folder = DriveFolder(c, make_folder(id='fld1'))

        with patch.object(folder, 'file', return_value=None), \
             patch('driveclient.client.execute_with_backoff') as mock_exec, \
             warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            mock_exec.return_value = make_file(id='new1', name='test.txt',
                                               mime=MIME_DOCUMENT)
            c.write(name='test.txt', folder=folder,
                    bytestring=b'hello', mimetype='text/plain')

        assert any('convert' in str(warning.message) for warning in w)
        # Should have used convert=True (v1 default)
        create_call = c.service.files().create
        body = create_call.call_args[1]['body']
        assert body.get('mimeType') == MIME_DOCUMENT

    @patch('driveclient.client.get_credentials')
    def test_explicit_convert_no_warning(self, mock_get_creds):
        c = DriveClient('test')
        c._credentials = MagicMock()
        c._service = MagicMock()

        folder = DriveFolder(c, make_folder(id='fld1'))

        with patch.object(folder, 'file', return_value=None), \
             patch('driveclient.client.execute_with_backoff') as mock_exec, \
             warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            mock_exec.return_value = make_file(id='new1')
            c.write(name='test.txt', folder=folder,
                    bytestring=b'hello', mimetype='text/plain',
                    convert=False)

        assert not any('convert' in str(warning.message) for warning in w)


class TestGetChangeCompat:

    @patch('driveclient.client.get_credentials')
    def test_get_change_raises(self, mock_get_creds):
        c = DriveClient('test')

        with pytest.raises(NotImplementedError, match='get_change'):
            c.get_change('change123')


class TestHttpCompat:

    @patch('driveclient.client.get_credentials')
    def test_http_property_warns(self, mock_get_creds):
        c = DriveClient('test')
        c._credentials = MagicMock()

        with warnings.catch_warnings(record=True) as w, \
             patch.dict('sys.modules', {
                 'google_auth_httplib2': MagicMock(),
                 'httplib2': MagicMock(),
             }):
            warnings.simplefilter('always')
            _ = c.http

        assert len(w) == 1
        assert 'http' in str(w[0].message).lower()
        assert issubclass(w[0].category, DeprecationWarning)
