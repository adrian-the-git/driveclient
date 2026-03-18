"""Shared fixtures for driveclient tests."""

import pytest
from unittest.mock import MagicMock, patch

from driveclient import DriveClient
from driveclient._fields import MIME_FOLDER, MIME_DOCUMENT, MIME_SPREADSHEET


# -- Sample metadata dicts (what the Drive API returns) --

def make_file(id='file1', name='test.txt', mime='text/plain', **extra):
    attrs = {
        'id': id,
        'name': name,
        'mimeType': mime,
        'parents': ['parent1'],
        'modifiedTime': '2025-01-01T00:00:00Z',
        'createdTime': '2025-01-01T00:00:00Z',
        'size': '1024',
        'md5Checksum': 'abc123',
        'trashed': False,
        'webViewLink': f'https://drive.google.com/file/d/{id}/view',
        'webContentLink': f'https://drive.google.com/uc?id={id}',
    }
    attrs.update(extra)
    return attrs


def make_folder(id='folder1', name='Test Folder'):
    return make_file(id=id, name=name, mime=MIME_FOLDER)


def make_doc(id='doc1', name='Test Doc'):
    return make_file(id=id, name=name, mime=MIME_DOCUMENT)


def make_spreadsheet_meta(id='sheet1', name='Test Sheet'):
    return make_file(id=id, name=name, mime=MIME_SPREADSHEET)


@pytest.fixture
def mock_creds():
    """A mock credentials object."""
    return MagicMock()


@pytest.fixture
def mock_drive_service():
    """A mock Drive API v3 service."""
    return MagicMock()


@pytest.fixture
def client(mock_creds, mock_drive_service):
    """A DriveClient with mocked credentials and service."""
    with patch('driveclient.client.get_credentials', return_value=mock_creds):
        c = DriveClient('test')
        c._credentials = mock_creds
        c._service = mock_drive_service
        return c
