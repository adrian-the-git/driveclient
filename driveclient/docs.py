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

        if tab_id:
            source_tab = source.tab(tab_id)
        else:
            tabs = source.tabs
            source_tab = tabs[0] if tabs else None

        if not source_tab:
            return

        lists = source._data.get('lists') if source._data else None
        requests = _build_copy_requests(source_tab, target_tab_id=None, lists=lists)

        if requests:
            return self.execute(
                self.service.documents().batchUpdate(
                    documentId=target_id,
                    body={'requests': requests}
                )
            )

    def copy_to_tab(self, source_document_id, target_document_id,
                    target_tab_id, source_tab_id=None):
        """
        Copy formatted content from a source document into a target tab.

        Fetches the source document, extracts the specified tab (or first tab),
        builds insert + style requests, and executes a batchUpdate on the target.
        """
        source = self.get(source_document_id)
        if not source:
            return

        if source_tab_id:
            source_tab = source.tab(source_tab_id)
        else:
            tabs = source.tabs
            source_tab = tabs[0] if tabs else None

        if not source_tab:
            return

        lists = source._data.get('lists') if source._data else None
        requests = _build_copy_requests(source_tab, target_tab_id=target_tab_id,
                                        lists=lists)

        if requests:
            return self.execute(
                self.service.documents().batchUpdate(
                    documentId=target_document_id,
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

    def add_tab(self, title, index=None):
        """Create a new tab and return its tabId."""
        tab_properties = {'title': title}
        if index is not None:
            tab_properties['index'] = index
        request = {'addDocumentTab': {'tabProperties': tab_properties}}
        result = self.batch_update([request])
        self._data = None  # invalidate cache
        tab_id = (result.get('replies', [{}])[0]
                  .get('addDocumentTab', {})
                  .get('tabProperties', {})
                  .get('tabId'))
        return tab_id

    def import_document(self, source_document_id, tab_title):
        """Create a new tab and copy source document's content into it.
        Returns the new tab_id."""
        tab_id = self.add_tab(tab_title)
        self.docs_service.copy_to_tab(
            source_document_id, self.document_id, tab_id
        )
        return tab_id

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


_WRITABLE_TEXT_STYLE_FIELDS = frozenset({
    'bold', 'italic', 'underline', 'strikethrough',
    'foregroundColor', 'backgroundColor',
    'fontSize', 'weightedFontFamily',
    'link', 'baselineOffset', 'smallCaps',
})

_WRITABLE_PARAGRAPH_STYLE_FIELDS = frozenset({
    'namedStyleType', 'alignment', 'lineSpacing',
    'spaceAbove', 'spaceBelow',
    'indentFirstLine', 'indentStart', 'indentEnd',
    'direction', 'spacingMode',
    'keepLinesTogether', 'keepWithNext',
    'avoidWidowAndOrphan',
})


def _utf16_len(text):
    """Count UTF-16 code units in text (for Docs API index tracking)."""
    count = 0
    for ch in text:
        code = ord(ch)
        if code > 0xFFFF:
            count += 2
        else:
            count += 1
    return count


def _build_text_style_fields(text_style):
    """Build a fields mask string from keys present in a textStyle dict."""
    if not text_style:
        return ''
    return ','.join(
        sorted(k for k in text_style if k in _WRITABLE_TEXT_STYLE_FIELDS)
    )


def _build_paragraph_style_fields(paragraph_style):
    """Build a fields mask string from keys present in a paragraphStyle dict."""
    if not paragraph_style:
        return ''
    return ','.join(
        sorted(k for k in paragraph_style if k in _WRITABLE_PARAGRAPH_STYLE_FIELDS)
    )


def _resolve_bullet_preset(list_id, lists):
    """Map a source list's glyphType to a bulletPreset string."""
    if not lists or not list_id:
        return 'BULLET_DISC_CIRCLE_SQUARE'
    list_data = lists.get(list_id)
    if not list_data:
        return 'BULLET_DISC_CIRCLE_SQUARE'
    nesting_levels = list_data.get('listProperties', {}).get('nestingLevels', [])
    if not nesting_levels:
        return 'BULLET_DISC_CIRCLE_SQUARE'
    glyph_type = nesting_levels[0].get('glyphType', '')
    ordered_types = {'DECIMAL', 'ALPHA', 'ROMAN',
                     'UPPER_ALPHA', 'UPPER_ROMAN',
                     'ZERO_DECIMAL'}
    if glyph_type in ordered_types:
        return 'NUMBERED_DECIMAL_ALPHA_ROMAN'
    return 'BULLET_DISC_CIRCLE_SQUARE'


def _build_copy_requests(source_tab, target_tab_id=None, lists=None):
    """
    Build batchUpdate requests to reproduce a source tab's content.

    Returns insert requests followed by style requests. Inserts use
    endOfSegmentLocation so they always append. Style requests reference
    tracked index ranges computed from insertion offsets.
    """
    insert_requests = []
    style_requests = []
    offset = 1  # doc body starts with a structural element at index 0

    elements = source_tab.structural_elements

    for element in elements:
        paragraph = element.get('paragraph')
        if not paragraph:
            continue

        para_start = offset
        paragraph_style = paragraph.get('paragraphStyle')
        bullet = paragraph.get('bullet')

        for pe in paragraph.get('elements', []):
            text_run = pe.get('textRun')
            if not text_run:
                continue
            content = text_run.get('content', '')
            if not content:
                continue

            run_start = offset
            run_len = _utf16_len(content)

            location = {}
            if target_tab_id is not None:
                location['tabId'] = target_tab_id

            insert_requests.append({
                'insertText': {
                    'endOfSegmentLocation': location,
                    'text': content,
                }
            })

            offset += run_len

            text_style = text_run.get('textStyle')
            if text_style:
                fields = _build_text_style_fields(text_style)
                if fields:
                    range_spec = {
                        'startIndex': run_start,
                        'endIndex': run_start + run_len,
                    }
                    if target_tab_id is not None:
                        range_spec['tabId'] = target_tab_id
                    style_requests.append({
                        'updateTextStyle': {
                            'textStyle': text_style,
                            'fields': fields,
                            'range': range_spec,
                        }
                    })

        para_end = offset

        if paragraph_style:
            fields = _build_paragraph_style_fields(paragraph_style)
            if fields:
                range_spec = {
                    'startIndex': para_start,
                    'endIndex': para_end,
                }
                if target_tab_id is not None:
                    range_spec['tabId'] = target_tab_id
                style_requests.append({
                    'updateParagraphStyle': {
                        'paragraphStyle': paragraph_style,
                        'fields': fields,
                        'range': range_spec,
                    }
                })

        if bullet:
            list_id = bullet.get('listId')
            preset = _resolve_bullet_preset(list_id, lists)
            range_spec = {
                'startIndex': para_start,
                'endIndex': para_end,
            }
            if target_tab_id is not None:
                range_spec['tabId'] = target_tab_id
            style_requests.append({
                'createParagraphBullets': {
                    'bulletPreset': preset,
                    'range': range_spec,
                }
            })

    return insert_requests + style_requests
