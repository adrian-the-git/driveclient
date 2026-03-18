"""
MIME type constants and Drive v3 field specifications.
"""

# Google Workspace MIME types
MIME_FOLDER = 'application/vnd.google-apps.folder'
MIME_DOCUMENT = 'application/vnd.google-apps.document'
MIME_SPREADSHEET = 'application/vnd.google-apps.spreadsheet'
MIME_PRESENTATION = 'application/vnd.google-apps.presentation'
MIME_DRAWING = 'application/vnd.google-apps.drawing'
MIME_FORM = 'application/vnd.google-apps.form'

# Common export MIME types
MIME_PDF = 'application/pdf'
MIME_PLAIN = 'text/plain'
MIME_HTML = 'text/html'
MIME_CSV = 'text/csv'
MIME_DOCX = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
MIME_XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
MIME_ODS = 'application/x-vnd.oasis.opendocument.spreadsheet'
MIME_ODT = 'application/vnd.oasis.opendocument.text'

# Image MIME types
MIME_JPEG = 'image/jpeg'
MIME_PNG = 'image/png'
MIME_GIF = 'image/gif'
MIME_TIFF = 'image/tiff'
MIME_SVG = 'image/svg+xml'
IMAGE_TYPES = [MIME_JPEG, MIME_PNG, MIME_GIF, MIME_TIFF, MIME_SVG]

# Drive v3 fields specification — v3 returns nothing unless you ask
FILE_FIELDS = (
    'id, name, mimeType, parents, modifiedTime, createdTime, size, '
    'md5Checksum, trashed, webViewLink, webContentLink, '
    'exportLinks, description, starred'
)
LIST_FIELDS = f'nextPageToken, files({FILE_FIELDS})'

# v2 → v3 attribute name mapping (for backward-compat aliases)
V2_TO_V3 = {
    'title': 'name',
    'modifiedDate': 'modifiedTime',
    'createdDate': 'createdTime',
    'downloadUrl': 'webContentLink',
}
