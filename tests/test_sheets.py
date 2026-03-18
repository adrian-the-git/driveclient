"""Tests for Google Sheets API integration."""

from unittest.mock import MagicMock, patch

import pytest

from driveclient.sheets import SheetsService, Spreadsheet, Sheet


SAMPLE_SPREADSHEET_DATA = {
    'properties': {'title': 'My Spreadsheet'},
    'sheets': [
        {'properties': {'sheetId': 0, 'title': 'Sheet1', 'index': 0}},
        {'properties': {'sheetId': 1, 'title': 'Sheet2', 'index': 1}},
    ],
}


@pytest.fixture
def sheets_service():
    creds = MagicMock()
    svc = SheetsService(creds)
    svc._service = MagicMock()
    return svc


@pytest.fixture
def spreadsheet(sheets_service):
    return Spreadsheet(sheets_service, 'ss_id', SAMPLE_SPREADSHEET_DATA)


class TestSheetsServiceGet:

    def test_get_returns_spreadsheet(self, sheets_service):
        with patch('driveclient.sheets.execute_with_backoff', return_value=SAMPLE_SPREADSHEET_DATA):
            ss = sheets_service.get('ss_id')

        assert isinstance(ss, Spreadsheet)
        assert ss.title == 'My Spreadsheet'

    def test_get_returns_none_if_not_found(self, sheets_service):
        with patch('driveclient.sheets.execute_with_backoff', return_value=None):
            ss = sheets_service.get('missing')

        assert ss is None


class TestSpreadsheet:

    def test_title(self, spreadsheet):
        assert spreadsheet.title == 'My Spreadsheet'

    def test_sheets_list(self, spreadsheet):
        sheets = spreadsheet.sheets
        assert len(sheets) == 2
        assert sheets[0].title == 'Sheet1'
        assert sheets[1].title == 'Sheet2'

    def test_sheet_by_title(self, spreadsheet):
        s = spreadsheet.sheet(title='Sheet2')
        assert s is not None
        assert s.sheet_id == 1

    def test_sheet_by_id(self, spreadsheet):
        s = spreadsheet.sheet(sheet_id=0)
        assert s is not None
        assert s.title == 'Sheet1'

    def test_sheet_not_found(self, spreadsheet):
        assert spreadsheet.sheet(title='Missing') is None

    def test_repr(self, spreadsheet):
        assert repr(spreadsheet) == '<Spreadsheet "My Spreadsheet">'


class TestSpreadsheetValues:

    def test_read_values(self, spreadsheet, sheets_service):
        with patch('driveclient.sheets.execute_with_backoff',
                   return_value={'values': [['a', 'b'], ['1', '2']]}):
            result = spreadsheet.values('Sheet1!A1:B2')

        assert result == [['a', 'b'], ['1', '2']]

    def test_read_empty_range(self, spreadsheet, sheets_service):
        with patch('driveclient.sheets.execute_with_backoff', return_value={}):
            result = spreadsheet.values('Sheet1!A1:B2')

        assert result == []

    def test_update_values(self, spreadsheet, sheets_service):
        with patch('driveclient.sheets.execute_with_backoff') as mock_exec:
            spreadsheet.update('Sheet1!A1:B2', [['x', 'y'], ['1', '2']])

        vals_update = sheets_service.service.spreadsheets().values().update
        call_kwargs = vals_update.call_args[1]
        assert call_kwargs['valueInputOption'] == 'USER_ENTERED'
        assert call_kwargs['body'] == {'values': [['x', 'y'], ['1', '2']]}

    def test_append_values(self, spreadsheet, sheets_service):
        with patch('driveclient.sheets.execute_with_backoff') as mock_exec:
            spreadsheet.append('Sheet1!A1', [['new_row']])

        vals_append = sheets_service.service.spreadsheets().values().append
        call_kwargs = vals_append.call_args[1]
        assert call_kwargs['valueInputOption'] == 'USER_ENTERED'


class TestSpreadsheetSheetManagement:

    def test_add_sheet(self, spreadsheet, sheets_service):
        with patch('driveclient.sheets.execute_with_backoff', return_value={'replies': []}):
            spreadsheet.add_sheet('NewSheet')

        batch = sheets_service.service.spreadsheets().batchUpdate
        body = batch.call_args[1]['body']
        assert body['requests'][0]['addSheet']['properties']['title'] == 'NewSheet'
        # Cached data should be invalidated
        assert spreadsheet._data is None

    def test_delete_sheet(self, spreadsheet, sheets_service):
        with patch('driveclient.sheets.execute_with_backoff', return_value={'replies': []}):
            spreadsheet.delete_sheet(1)

        batch = sheets_service.service.spreadsheets().batchUpdate
        body = batch.call_args[1]['body']
        assert body['requests'][0]['deleteSheet']['sheetId'] == 1
        assert spreadsheet._data is None


class TestSheet:

    def test_properties(self, spreadsheet):
        s = spreadsheet.sheets[0]
        assert s.sheet_id == 0
        assert s.title == 'Sheet1'
        assert s.index == 0

    def test_values_full_sheet(self, spreadsheet):
        s = spreadsheet.sheets[0]
        with patch.object(spreadsheet, 'values', return_value=[['a']]) as mock_vals:
            result = s.values()

        mock_vals.assert_called_once_with("'Sheet1'")
        assert result == [['a']]

    def test_values_with_range(self, spreadsheet):
        s = spreadsheet.sheets[0]
        with patch.object(spreadsheet, 'values', return_value=[]) as mock_vals:
            s.values('A1:C10')

        mock_vals.assert_called_once_with("'Sheet1'!A1:C10")

    def test_update(self, spreadsheet):
        s = spreadsheet.sheets[0]
        with patch.object(spreadsheet, 'update') as mock_update:
            s.update('A1:B2', [['x', 'y']])

        mock_update.assert_called_once_with("'Sheet1'!A1:B2", [['x', 'y']])

    def test_append(self, spreadsheet):
        s = spreadsheet.sheets[0]
        with patch.object(spreadsheet, 'append') as mock_append:
            s.append('A1', [['new']])

        mock_append.assert_called_once_with("'Sheet1'!A1", [['new']])

    def test_clear(self, spreadsheet, sheets_service):
        s = spreadsheet.sheets[0]
        with patch('driveclient.sheets.execute_with_backoff'):
            s.clear()

        clear_call = sheets_service.service.spreadsheets().values().clear
        call_kwargs = clear_call.call_args[1]
        assert call_kwargs['range'] == "'Sheet1'"

    def test_clear_with_range(self, spreadsheet, sheets_service):
        s = spreadsheet.sheets[0]
        with patch('driveclient.sheets.execute_with_backoff'):
            s.clear('A1:B5')

        clear_call = sheets_service.service.spreadsheets().values().clear
        call_kwargs = clear_call.call_args[1]
        assert call_kwargs['range'] == "'Sheet1'!A1:B5"

    def test_repr(self, spreadsheet):
        s = spreadsheet.sheets[0]
        assert repr(s) == '<Sheet "Sheet1">'


class TestSheetDataFrame:

    def test_to_dataframe(self, spreadsheet):
        s = spreadsheet.sheets[0]
        with patch.object(s, 'values', return_value=[['name', 'age'], ['Alice', '30']]):
            df = s.to_dataframe()

        assert list(df.columns) == ['name', 'age']
        assert len(df) == 1
        assert df.iloc[0]['name'] == 'Alice'

    def test_to_dataframe_no_header(self, spreadsheet):
        s = spreadsheet.sheets[0]
        with patch.object(s, 'values', return_value=[['Alice', '30'], ['Bob', '25']]):
            df = s.to_dataframe(header=False)

        assert len(df) == 2

    def test_to_dataframe_empty(self, spreadsheet):
        s = spreadsheet.sheets[0]
        with patch.object(s, 'values', return_value=[]):
            df = s.to_dataframe()

        assert len(df) == 0

    def test_from_dataframe(self, spreadsheet):
        import pandas as pd
        s = spreadsheet.sheets[0]
        df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})

        with patch.object(s, 'clear') as mock_clear, \
             patch.object(s, 'update') as mock_update:
            s.from_dataframe(df)

        mock_clear.assert_called_once()
        mock_update.assert_called_once()
        values = mock_update.call_args[0][1]
        assert values[0] == ['name', 'age']  # header
        assert values[1] == ['Alice', 30]
