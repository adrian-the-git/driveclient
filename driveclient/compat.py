"""
Backward-compatibility shim for driveclient v1 → v2 migration.

Import this module once at startup to hotpatch the v2 API with v1-compatible
behavior. All shimmed behavior emits DeprecationWarnings so callers can find
and update their code.

    import driveclient.compat  # activates shims

This module will refuse to load after 2026-07-01.
"""

import datetime
import functools
import warnings

EXPIRY = datetime.date(2026, 7, 1)

if datetime.date.today() >= EXPIRY:
    raise ImportError(
        "driveclient.compat expired on {}. "
        "Update your code to use the v2 API directly.".format(EXPIRY)
    )

from . import client, objects
from .client import DriveClient
from .objects import DriveFile, DriveFolder


def _warn(msg):
    warnings.warn(msg, DeprecationWarning, stacklevel=3)


# ---------------------------------------------------------------------------
# DriveClient.__init__: accept scopes as a single string
# ---------------------------------------------------------------------------

_original_init = DriveClient.__init__

@functools.wraps(_original_init)
def _patched_init(self, *args, **kwargs):
    scopes = kwargs.get('scopes')
    if isinstance(scopes, str):
        _warn("Passing scopes as a string is deprecated. Use a list: ['{}']".format(scopes))
        kwargs['scopes'] = [scopes]
    _original_init(self, *args, **kwargs)

DriveClient.__init__ = _patched_init


# ---------------------------------------------------------------------------
# DriveClient.query: accept maxResults as alias for page_size
# ---------------------------------------------------------------------------

_original_query = DriveClient.query

@functools.wraps(_original_query)
def _patched_query(self, q, parent=None, page_size=1000, **kwargs):
    if 'maxResults' in kwargs:
        _warn("maxResults is deprecated, use page_size instead.")
        page_size = kwargs.pop('maxResults')
    # Rewrite v2 field names in query strings
    if 'title=' in q or 'title ' in q:
        _warn('Query field "title" is deprecated in Drive v3. Use "name" instead.')
        q = q.replace('title=', 'name=').replace('title ', 'name ')
    return _original_query(self, q, parent=parent, page_size=page_size, **kwargs)

DriveClient.query = _patched_query


# ---------------------------------------------------------------------------
# DriveClient.write: default convert=True to match v1 behavior
# ---------------------------------------------------------------------------

_original_write = DriveClient.write

@functools.wraps(_original_write)
def _patched_write(self, name='', folder=None, bytestring=b'', mimetype='text/plain',
                   replace=True, convert=None, id=''):
    if convert is None:
        _warn("write() now defaults to convert=False. The compat shim is preserving "
              "the old convert=True default. Pass convert explicitly to silence this.")
        convert = True
    return _original_write(self, name=name, folder=folder, bytestring=bytestring,
                           mimetype=mimetype, replace=replace, convert=convert, id=id)

DriveClient.write = _patched_write


# ---------------------------------------------------------------------------
# DriveClient.http: stub that warns and provides a basic adapter
# ---------------------------------------------------------------------------

@property
def _http_shim(self):
    _warn("DriveClient.http is removed in v2. Use the service object directly.")
    try:
        return self._http_compat
    except AttributeError:
        import google_auth_httplib2
        import httplib2
        self._http_compat = google_auth_httplib2.AuthorizedHttp(
            self.credentials, http=httplib2.Http()
        )
        return self._http_compat

DriveClient.http = _http_shim


# ---------------------------------------------------------------------------
# DriveClient.get_change: stub that raises with a clear message
# ---------------------------------------------------------------------------

def _get_change_stub(self, changeId):
    raise NotImplementedError(
        "get_change() is not available in Drive API v3. "
        "Use the Changes API via client.service.changes() directly."
    )

DriveClient.get_change = _get_change_stub


# ---------------------------------------------------------------------------
# DriveFolder.file / folder: rewrite title= to name= in queries
# ---------------------------------------------------------------------------

_original_folder_file = DriveFolder.file

@functools.wraps(_original_folder_file)
def _patched_folder_file(self, name):
    # The method itself builds the query with name=, so this is fine.
    # But patch query() above handles any raw title= strings.
    return _original_folder_file(self, name)

# No patch needed here since the folder methods build their own queries
# with the correct v3 field names. The query() shim handles raw strings.
