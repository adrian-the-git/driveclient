"""
Authentication using google-auth and google-auth-oauthlib.

Replaces oauth2client with modern credential handling:
- InstalledAppFlow for interactive OAuth
- service_account.Credentials for service accounts
- JSON file storage for credential caching
"""

import json
import os

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/spreadsheets',
]

CLIENT_SECRET_FILENAME = 'client_secret.json'
CACHED_CREDENTIALS_DIRECTORY = '~/.credentials'


def get_credentials(name, client_secret_filename=CLIENT_SECRET_FILENAME,
                    cached_credentials_directory=CACHED_CREDENTIALS_DIRECTORY,
                    scopes=None, service_account_json_filename=None):
    """
    Obtain credentials, using cached credentials when available.

    For service accounts, loads directly from the JSON key file.
    For user accounts, uses InstalledAppFlow with a local server for OAuth.
    """
    scopes = scopes or DEFAULT_SCOPES

    if service_account_json_filename:
        return service_account.Credentials.from_service_account_file(
            service_account_json_filename, scopes=scopes
        )

    cached_credentials_directory = os.path.expanduser(cached_credentials_directory)
    os.makedirs(cached_credentials_directory, exist_ok=True)
    cached_path = os.path.join(cached_credentials_directory, name + '.json')

    creds = None
    if os.path.exists(cached_path):
        creds = Credentials.from_authorized_user_file(cached_path, scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secret_filename, scopes
            )
            creds = flow.run_local_server(port=0)
        with open(cached_path, 'w') as f:
            f.write(creds.to_json())

    return creds
