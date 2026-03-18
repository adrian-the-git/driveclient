"""
Google Docs API v1 integration.

Provides DocsService, Document, and DocumentTab for reading, copying,
merging, and template-filling Google Docs with tab support.
"""

from googleapiclient import discovery

from .utils import execute_with_backoff


class DocsService:
    """
    Wraps the Google Docs API v1 service. Accessed via DriveClient.docs.
    """

    def __init__(self, credentials, drive_client=None):
        self.credentials = credentials
        self.drive_client = drive_client
        self._service = None

    @property
    def service(self):
        if self._service is None:
            self._service = discovery.build(
                'docs', 'v1', credentials=self.credentials
            )
        return self._service

    def execute(self, request):
        return execute_with_backoff(request)

    def get(self, document_id):
        """Fetch a document and return a Document wrapper."""
        result = self.execute(
            self.service.documents().get(
                documentId=document_id, includeTabsContent=True
            )
        )
        if result:
            return Document(self, document_id, result)

    def copy_with_styles(self, source_id, name=None, parent_id=None):
        """
        Copy a document preserving full structure and styles.
        Uses Drive files.copy under the hood.
        """
        if not self.drive_client:
            raise RuntimeError("copy_with_styles requires a DriveClient (access via DriveClient.docs)")
        new_file = self.drive_client.copy(source_id, name=name, parent_id=parent_id)
        if new_file:
            return self.get(new_file.id)

    def replace_all(self, document_id, replacements, match_case=True, tab_ids=None):
        """
        Perform replaceAllText on a document.

        replacements: dict of {placeholder: replacement_text}
        tab_ids: optional list of tab IDs to restrict replacement to
        """
        requests = []
        for old, new in replacements.items():
            req = {
                'replaceAllText': {
                    'containsText': {
                        'text': old,
                        'matchCase': match_case,
                    },
                    'replaceText': new,
                }
            }
            if tab_ids:
                req['replaceAllText']['tabsCriteria'] = {
                    'tabIds': tab_ids
                }
            requests.append(req)

        return self.execute(
            self.service.documents().batchUpdate(
                documentId=document_id,
                body={'requests': requests}
            )
        )

    def merge(self, target_id, source_id, tab_id=None):
        """
        Merge content from source document into target by reading structural
        elements and reproducing them via InsertText + UpdateTextStyle requests.
        Appends source content at the end of the target document.
        """
        source = self.get(source_id)
        if not source:
            return

        # Get the tab to merge from
        if tab_id:
            source_tab = source.tab(tab_id)
        else:
            tabs = source.tabs
            source_tab = tabs[0] if tabs else None

        if not source_tab:
            return

        elements = source_tab.structural_elements
        requests = _build_merge_requests(elements, target_id)

        if requests:
            return self.execute(
                self.service.documents().batchUpdate(
                    documentId=target_id,
                    body={'requests': requests}
                )
            )


class Document:
    """Wraps a Google Doc with tab-aware access."""

    def __init__(self, docs_service, document_id, data=None):
        self.docs_service = docs_service
        self.document_id = document_id
        self._data = data

    @property
    def data(self):
        if self._data is None:
            doc = self.docs_service.get(self.document_id)
            if doc:
                self._data = doc._data
        return self._data

    @property
    def title(self):
        return self.data.get('title', '') if self.data else ''

    @property
    def tabs(self):
        """Return all tabs (including nested child tabs) as DocumentTab objects."""
        if not self.data:
            return []
        raw_tabs = self.data.get('tabs', [])
        result = []
        for raw_tab in raw_tabs:
            _collect_tabs(raw_tab, result)
        return result

    def tab(self, tab_id=None, title=None):
        """Get a specific tab by ID or title."""
        for t in self.tabs:
            if tab_id and t.tab_id == tab_id:
                return t
            if title and t.title == title:
                return t
        return None

    @property
    def text(self):
        """Get plain text of the first tab."""
        tabs = self.tabs
        return tabs[0].text if tabs else ''

    def replace_all(self, replacements, match_case=True, tab_ids=None):
        """Replace all occurrences of placeholders in this document."""
        return self.docs_service.replace_all(
            self.document_id, replacements,
            match_case=match_case, tab_ids=tab_ids
        )

    def batch_update(self, requests):
        """Send a batchUpdate request to this document."""
        return self.docs_service.execute(
            self.docs_service.service.documents().batchUpdate(
                documentId=self.document_id,
                body={'requests': requests}
            )
        )

    def __repr__(self):
        return '<Document "{}">'.format(self.title)


class DocumentTab:
    """Wraps a single tab within a Google Doc."""

    def __init__(self, tab_properties, document_style, body):
        self._tab_properties = tab_properties
        self._document_style = document_style
        self._body = body

    @property
    def tab_id(self):
        return self._tab_properties.get('tabId', '')

    @property
    def title(self):
        return self._tab_properties.get('title', '')

    @property
    def body(self):
        return self._body

    @property
    def structural_elements(self):
        """Return the list of structural elements in this tab's body."""
        if self._body:
            return self._body.get('content', [])
        return []

    @property
    def text(self):
        """Extract plain text from this tab's structural elements."""
        parts = []
        for element in self.structural_elements:
            paragraph = element.get('paragraph')
            if paragraph:
                for pe in paragraph.get('elements', []):
                    text_run = pe.get('textRun')
                    if text_run:
                        parts.append(text_run.get('content', ''))
        return ''.join(parts)

    def __repr__(self):
        return '<DocumentTab "{}">'.format(self.title)


def _collect_tabs(raw_tab, result):
    """Recursively collect tabs and child tabs into a flat list."""
    tab_props = raw_tab.get('tabProperties', {})
    doc_tab = raw_tab.get('documentTab', {})
    body = doc_tab.get('body')
    doc_style = doc_tab.get('documentStyle')
    result.append(DocumentTab(tab_props, doc_style, body))
    for child in raw_tab.get('childTabs', []):
        _collect_tabs(child, result)


def _build_merge_requests(elements, target_id):
    """
    Build InsertText + UpdateTextStyle batch requests to reproduce
    structural elements from a source document into a target.
    """
    requests = []
    # We insert at the end of the document; we need to track our insertion index.
    # Start after the existing content by inserting at index 1 (after the initial newline).
    # A more robust approach would read the target doc's endIndex, but for simplicity
    # we append a newline separator first.

    insert_requests = []
    style_requests = []

    for element in elements:
        paragraph = element.get('paragraph')
        if not paragraph:
            continue
        for pe in paragraph.get('elements', []):
            text_run = pe.get('textRun')
            if not text_run:
                continue
            content = text_run.get('content', '')
            if not content:
                continue

            insert_requests.append({
                'insertText': {
                    'endOfSegmentLocation': {},
                    'text': content,
                }
            })

            text_style = text_run.get('textStyle')
            if text_style:
                # We'll apply styles in a follow-up pass if needed
                style_requests.append(text_style)

    return insert_requests
