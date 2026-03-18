"""Tests for authentication flows."""

import json
import os
from unittest.mock import MagicMock, patch, mock_open

import pytest

from driveclient.auth import get_credentials, DEFAULT_SCOPES


class TestServiceAccountAuth:

    @patch('driveclient.auth.service_account.Credentials.from_service_account_file')
    def test_loads_from_json_file(self, mock_from_file):
        mock_creds = MagicMock()
        mock_from_file.return_value = mock_creds

        result = get_credentials('test', service_account_json_filename='sa.json')

        mock_from_file.assert_called_once_with('sa.json', scopes=DEFAULT_SCOPES)
        assert result is mock_creds

    @patch('driveclient.auth.service_account.Credentials.from_service_account_file')
    def test_custom_scopes(self, mock_from_file):
        scopes = ['https://www.googleapis.com/auth/drive.readonly']
        get_credentials('test', service_account_json_filename='sa.json', scopes=scopes)
        mock_from_file.assert_called_once_with('sa.json', scopes=scopes)


class TestOAuthFlow:

    @patch('driveclient.auth.os.path.exists', return_value=False)
    @patch('driveclient.auth.os.makedirs')
    @patch('driveclient.auth.InstalledAppFlow.from_client_secrets_file')
    @patch('builtins.open', mock_open())
    def test_new_oauth_runs_flow(self, mock_flow_cls, mock_makedirs, mock_exists):
        mock_creds = MagicMock()
        mock_creds.to_json.return_value = '{"token": "test"}'
        mock_flow = MagicMock()
        mock_flow.run_local_server.return_value = mock_creds
        mock_flow_cls.return_value = mock_flow

        result = get_credentials('myapp', client_secret_filename='secret.json')

        mock_flow_cls.assert_called_once_with('secret.json', DEFAULT_SCOPES)
        mock_flow.run_local_server.assert_called_once_with(port=0)
        assert result is mock_creds

    @patch('driveclient.auth.os.path.exists', return_value=True)
    @patch('driveclient.auth.os.makedirs')
    @patch('driveclient.auth.Credentials.from_authorized_user_file')
    def test_loads_cached_credentials(self, mock_from_file, mock_makedirs, mock_exists):
        mock_creds = MagicMock()
        mock_creds.valid = True
        mock_from_file.return_value = mock_creds

        result = get_credentials('myapp')

        assert result is mock_creds
        mock_from_file.assert_called_once()

    @patch('driveclient.auth.os.path.exists', return_value=True)
    @patch('driveclient.auth.os.makedirs')
    @patch('driveclient.auth.Credentials.from_authorized_user_file')
    @patch('driveclient.auth.Request')
    @patch('builtins.open', mock_open())
    def test_refreshes_expired_credentials(self, mock_request_cls, mock_from_file,
                                           mock_makedirs, mock_exists):
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = 'refresh_token'
        mock_creds.to_json.return_value = '{}'
        mock_from_file.return_value = mock_creds

        result = get_credentials('myapp')

        mock_creds.refresh.assert_called_once()
        assert result is mock_creds
