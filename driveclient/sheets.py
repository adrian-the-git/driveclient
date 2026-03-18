"""
Google Sheets API v4 integration.

Provides SheetsService, Spreadsheet, and Sheet for reading, writing,
and managing Google Sheets with optional pandas support.
"""

from googleapiclient import discovery

from ._compat import require_pandas
from .utils import execute_with_backoff


class SheetsService:
    """
    Wraps the Google Sheets API v4 service. Accessed via DriveClient.sheets.
    """

    def __init__(self, credentials, drive_client=None):
        self.credentials = credentials
        self.drive_client = drive_client
        self._service = None

    @property
    def service(self):
        if self._service is None:
            self._service = discovery.build(
                'sheets', 'v4', credentials=self.credentials
            )
        return self._service

    def execute(self, request):
        return execute_with_backoff(request)

    def get(self, spreadsheet_id):
        """Fetch a spreadsheet and return a Spreadsheet wrapper."""
        result = self.execute(
            self.service.spreadsheets().get(spreadsheetId=spreadsheet_id)
        )
        if result:
            return Spreadsheet(self, spreadsheet_id, result)


class Spreadsheet:
    """Wraps a Google Spreadsheet with sheet-level access."""

    def __init__(self, sheets_service, spreadsheet_id, data=None):
        self.sheets_service = sheets_service
        self.spreadsheet_id = spreadsheet_id
        self._data = data

    @property
    def data(self):
        if self._data is None:
            ss = self.sheets_service.get(self.spreadsheet_id)
            if ss:
                self._data = ss._data
        return self._data

    @property
    def title(self):
        return self.data.get('properties', {}).get('title', '') if self.data else ''

    @property
    def sheets(self):
        """Return all sheets as Sheet objects."""
        if not self.data:
            return []
        return [
            Sheet(self, s['properties'])
            for s in self.data.get('sheets', [])
        ]

    def sheet(self, title=None, sheet_id=None):
        """Get a specific sheet by title or ID."""
        for s in self.sheets:
            if title and s.title == title:
                return s
            if sheet_id is not None and s.sheet_id == sheet_id:
                return s
        return None

    def values(self, range_):
        """Read values from a range (e.g. 'Sheet1!A1:C10')."""
        result = self.sheets_service.execute(
            self.sheets_service.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range=range_
            )
        )
        return result.get('values', []) if result else []

    def update(self, range_, values):
        """Write values to a range."""
        return self.sheets_service.execute(
            self.sheets_service.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id, range=range_,
                valueInputOption='USER_ENTERED',
                body={'values': values}
            )
        )

    def append(self, range_, values):
        """Append values after a range."""
        return self.sheets_service.execute(
            self.sheets_service.service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id, range=range_,
                valueInputOption='USER_ENTERED',
                body={'values': values}
            )
        )

    def add_sheet(self, title):
        """Add a new sheet tab."""
        result = self.sheets_service.execute(
            self.sheets_service.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={'requests': [{'addSheet': {'properties': {'title': title}}}]}
            )
        )
        # Invalidate cached data
        self._data = None
        return result

    def delete_sheet(self, sheet_id):
        """Delete a sheet tab by its ID."""
        result = self.sheets_service.execute(
            self.sheets_service.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={'requests': [{'deleteSheet': {'sheetId': sheet_id}}]}
            )
        )
        self._data = None
        return result

    def batch_update(self, requests):
        """Send a batchUpdate request to this spreadsheet."""
        return self.sheets_service.execute(
            self.sheets_service.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={'requests': requests}
            )
        )

    def __repr__(self):
        return '<Spreadsheet "{}">'.format(self.title)


class Sheet:
    """Wraps an individual sheet within a spreadsheet."""

    def __init__(self, spreadsheet, properties):
        self.spreadsheet = spreadsheet
        self._properties = properties

    @property
    def sheet_id(self):
        return self._properties.get('sheetId')

    @property
    def title(self):
        return self._properties.get('title', '')

    @property
    def index(self):
        return self._properties.get('index', 0)

    def values(self, range_=None):
        """Read values from this sheet, optionally with a cell range like 'A1:C10'."""
        full_range = "'{}'".format(self.title)
        if range_:
            full_range = "'{}'!{}".format(self.title, range_)
        return self.spreadsheet.values(full_range)

    def update(self, range_, values):
        """Write values to a range within this sheet."""
        full_range = "'{}'!{}".format(self.title, range_)
        return self.spreadsheet.update(full_range, values)

    def append(self, range_, values):
        """Append values after a range within this sheet."""
        full_range = "'{}'!{}".format(self.title, range_)
        return self.spreadsheet.append(full_range, values)

    def clear(self, range_=None):
        """Clear values from this sheet."""
        full_range = "'{}'".format(self.title)
        if range_:
            full_range = "'{}'!{}".format(self.title, range_)
        return self.spreadsheet.sheets_service.execute(
            self.spreadsheet.sheets_service.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet.spreadsheet_id,
                range=full_range, body={}
            )
        )

    def to_dataframe(self, header=True):
        """
        Convert this sheet's data to a pandas DataFrame.
        Raises ImportError if pandas is not installed.
        """
        pd = require_pandas()
        rows = self.values()
        if not rows:
            return pd.DataFrame()
        if header:
            return pd.DataFrame(rows[1:], columns=rows[0])
        return pd.DataFrame(rows)

    def from_dataframe(self, df, include_header=True):
        """
        Write a pandas DataFrame to this sheet (overwrites existing content).
        Raises ImportError if pandas is not installed.
        """
        require_pandas()
        values = []
        if include_header:
            values.append(list(df.columns.astype(str)))
        for _, row in df.iterrows():
            values.append([
                v if not _is_na(v) else ''
                for v in row.tolist()
            ])
        self.clear()
        if values:
            self.update('A1', values)

    def __repr__(self):
        return '<Sheet "{}">'.format(self.title)


def _is_na(value):
    """Check if a value is NA/NaN without requiring pandas at module level."""
    try:
        import math
        if isinstance(value, float) and math.isnan(value):
            return True
    except (TypeError, ValueError):
        pass
    try:
        import pandas as pd
        if pd.isna(value):
            return True
    except Exception:
        pass
    return False
