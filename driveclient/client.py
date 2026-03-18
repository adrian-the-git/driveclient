"""
DriveClient — main entry point for Drive API v3.

Handles authentication, service construction, and provides methods to
fetch, query, copy, and write files.
"""

from io import BytesIO

from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseUpload

from ._fields import (
    FILE_FIELDS, LIST_FIELDS, MIME_DOCUMENT, MIME_FOLDER, MIME_SPREADSHEET,
)
from .auth import (
    CLIENT_SECRET_FILENAME, CACHED_CREDENTIALS_DIRECTORY, DEFAULT_SCOPES,
    get_credentials,
)
from .objects import DriveObject
from .utils import debug_log, execute_with_backoff


class DriveClient:
    """
    Connects to Google's Drive API v3 and provides methods to fetch files or
    folders by id or custom query. Lazy properties provide access to the
    Docs and Sheets sub-services.
    """

    def __init__(self, name, client_secret_filename=CLIENT_SECRET_FILENAME,
                 cached_credentials_directory=CACHED_CREDENTIALS_DIRECTORY,
                 scopes=None, service_account_json_filename=None):
        """
        If service_account_json_filename is provided, a service account key
        is used instead of the interactive OAuth flow.
        """
        self.name = name
        self.client_secret_filename = client_secret_filename
        self.cached_credentials_directory = cached_credentials_directory
        self.scopes = scopes or DEFAULT_SCOPES
        self.service_account_json_filename = service_account_json_filename
        self._credentials = None
        self._service = None
        self._docs_service = None
        self._sheets_service = None

    @property
    def credentials(self):
        if self._credentials is None:
            self._credentials = get_credentials(
                self.name,
                client_secret_filename=self.client_secret_filename,
                cached_credentials_directory=self.cached_credentials_directory,
                scopes=self.scopes,
                service_account_json_filename=self.service_account_json_filename,
            )
        return self._credentials

    @property
    def service(self):
        """The Drive API v3 service object."""
        if self._service is None:
            self._service = discovery.build(
                'drive', 'v3', credentials=self.credentials
            )
        return self._service

    @property
    def docs(self):
        """Lazy DocsService accessor."""
        if self._docs_service is None:
            from .docs import DocsService
            self._docs_service = DocsService(self.credentials, self)
        return self._docs_service

    @property
    def sheets(self):
        """Lazy SheetsService accessor."""
        if self._sheets_service is None:
            from .sheets import SheetsService
            self._sheets_service = SheetsService(self.credentials, self)
        return self._sheets_service

    def execute(self, request):
        """Execute a request with exponential backoff."""
        return execute_with_backoff(request)

    # -- Fetching --

    @property
    def root(self):
        """Return the root folder (uses Drive v3 'root' alias)."""
        result = self.execute(
            self.service.files().get(fileId='root', fields=FILE_FIELDS)
        )
        if result:
            return DriveObject(self, result)

    def get(self, id):
        """Get a file by its globally unique id."""
        result = self.execute(
            self.service.files().get(fileId=id, fields=FILE_FIELDS)
        )
        if result:
            return DriveObject(self, result)

    def query(self, q, parent=None, page_size=1000):
        """
        Perform a query with full pagination support.
        Returns a list of DriveObjects, or a single DriveObject when page_size=1.
        """
        if parent:
            parent_id = parent.id if isinstance(parent, DriveObject) else parent
            q = '"{}" in parents and ({})'.format(parent_id, q)

        all_files = []
        page_token = None

        while True:
            params = {
                'pageSize': min(page_size, 1000),
                'orderBy': 'modifiedTime desc',
                'q': q,
                'fields': LIST_FIELDS,
            }
            if page_token:
                params['pageToken'] = page_token

            result = self.execute(self.service.files().list(**params))
            if not result:
                break

            all_files.extend(
                DriveObject(self, f) for f in result.get('files', [])
            )

            if page_size <= 1 and all_files:
                return all_files[0]

            page_token = result.get('nextPageToken')
            if not page_token or len(all_files) >= page_size:
                break

        if page_size <= 1:
            return all_files[0] if all_files else None
        return all_files

    def file(self, name='', id=''):
        """Get a single file by name or id."""
        if id:
            return self.get(id)
        q = 'name="{}" and mimeType!="{}" and trashed=false'.format(name, MIME_FOLDER)
        return self.query(q, page_size=1)

    def folder(self, name='', id=''):
        """Get a single folder by name or id."""
        if id:
            return self.get(id)
        q = 'name="{}" and mimeType="{}" and trashed=false'.format(name, MIME_FOLDER)
        return self.query(q, page_size=1)

    # -- Copying --

    def copy(self, file_id, name=None, parent_id=None):
        """
        Copy a file (preserves full document structure for Docs/Sheets).
        Returns the new DriveObject.
        """
        body = {}
        if name:
            body['name'] = name
        if parent_id:
            body['parents'] = [parent_id]
        result = self.execute(
            self.service.files().copy(fileId=file_id, body=body, fields=FILE_FIELDS)
        )
        if result:
            return DriveObject(self, result)

    # -- Writing --

    def write(self, name='', folder=None, bytestring=b'', mimetype='text/plain',
              replace=True, convert=False, id=''):
        """
        Write file data (given as bytes). Specify either a filename and folder
        OR a file id.

        When convert=True, the file is uploaded as a Google Workspace document
        by setting the appropriate target mimeType.
        """
        target_mimetype = None
        if convert:
            target_mimetype = _guess_workspace_type(mimetype)

        if id:
            existing_file = self.file(id=id)
            if not existing_file:
                return
            name = existing_file.name
            parents = existing_file.attributes.get('parents')
        elif name:
            if isinstance(folder, str):
                folder = self.folder(name=folder)
                if not folder:
                    return
            elif not folder:
                folder = self.root
            existing_file = folder.file(name)
            parents = [folder.id]
        else:
            return

        media = MediaIoBaseUpload(BytesIO(bytestring), mimetype=mimetype)

        if existing_file and not replace:
            debug_log('not replacing "{}"'.format(name))
            return

        if existing_file:
            # In v3, if conversion type changed, delete and recreate
            is_workspace = 'google-apps' in existing_file.mimeType
            if (not convert and is_workspace) or (convert and not is_workspace):
                try:
                    debug_log('deleting "{}" for type conversion'.format(name))
                    self.execute(self.service.files().delete(fileId=existing_file.id))
                except Exception:
                    debug_log('can\'t replace "{}" for type conversion'.format(name))
                    return
            else:
                debug_log('updating "{}"'.format(name))
                body = {'name': name}
                result = self.execute(
                    self.service.files().update(
                        fileId=existing_file.id, body=body,
                        media_body=media, fields=FILE_FIELDS
                    )
                )
                if result:
                    return DriveObject(self, result)
                return

        debug_log('creating "{}"'.format(name))
        body = {'name': name, 'parents': parents}
        if target_mimetype:
            body['mimeType'] = target_mimetype
        result = self.execute(
            self.service.files().create(
                body=body, media_body=media, fields=FILE_FIELDS
            )
        )
        if result:
            return DriveObject(self, result)


def _guess_workspace_type(mimetype):
    """Map an upload MIME type to a Google Workspace target type for conversion."""
    if mimetype in ('text/plain', 'text/html', 'application/rtf'):
        return MIME_DOCUMENT
    if mimetype in ('text/csv', 'application/vnd.ms-excel'):
        return MIME_SPREADSHEET
    return MIME_DOCUMENT  # default to Doc
