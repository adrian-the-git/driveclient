"""
driveclient — a simple Google Drive, Docs, and Sheets API client.

Modernized for Drive API v3, Docs API v1, and Sheets API v4.
"""

from .client import DriveClient
from .objects import DriveObject, DriveFile, DriveFolder
from .docs import DocsService, Document, DocumentTab
from .sheets import SheetsService, Spreadsheet, Sheet
from .utils import hashfile

__all__ = [
    'DriveClient',
    'DriveObject',
    'DriveFile',
    'DriveFolder',
    'DocsService',
    'Document',
    'DocumentTab',
    'SheetsService',
    'Spreadsheet',
    'Sheet',
    'hashfile',
]
