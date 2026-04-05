"""Tests for Google Docs API integration."""

from unittest.mock import MagicMock, patch, call

import pytest

from driveclient.docs import (
    DocsService, Document, DocumentTab,
    _utf16_len, _build_text_style_fields, _build_paragraph_style_fields,
    _resolve_bullet_preset, _build_copy_requests,
)


SAMPLE_DOC_DATA = {
    'title': 'My Document',
    'tabs': [
        {
            'tabProperties': {'tabId': 'tab1', 'title': 'Main'},
            'documentTab': {
                'body': {
                    'content': [
                        {
                            'paragraph': {
                                'elements': [
                                    {'textRun': {'content': 'Hello '}}
                                ]
                            }
                        },
                        {
                            'paragraph': {
                                'elements': [
                                    {'textRun': {'content': 'World\n'}}
                                ]
                            }
                        },
                    ]
                },
                'documentStyle': {},
            },
            'childTabs': [
                {
                    'tabProperties': {'tabId': 'tab2', 'title': 'Child Tab'},
                    'documentTab': {
                        'body': {
                            'content': [
                                {
                                    'paragraph': {
                                        'elements': [
                                            {'textRun': {'content': 'Child content\n'}}
                                        ]
                                    }
                                }
                            ]
                        },
                        'documentStyle': {},
                    },
                    'childTabs': [],
                }
            ],
        }
    ],
}


@pytest.fixture
def docs_service():
    creds = MagicMock()
    drive_client = MagicMock()
    svc = DocsService(creds, drive_client)
    svc._service = MagicMock()
    return svc


class TestDocsServiceGet:

    def test_get_returns_document(self, docs_service):
        with patch('driveclient.docs.execute_with_backoff', return_value=SAMPLE_DOC_DATA):
            doc = docs_service.get('doc_id_1')

        assert isinstance(doc, Document)
        assert doc.title == 'My Document'

    def test_get_returns_none_if_not_found(self, docs_service):
        with patch('driveclient.docs.execute_with_backoff', return_value=None):
            doc = docs_service.get('missing')

        assert doc is None


class TestDocument:

    def test_title(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        assert doc.title == 'My Document'

    def test_tabs_includes_children(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        tabs = doc.tabs

        assert len(tabs) == 2
        assert tabs[0].title == 'Main'
        assert tabs[1].title == 'Child Tab'

    def test_text_returns_first_tab_text(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        assert doc.text == 'Hello World\n'

    def test_tab_by_id(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        tab = doc.tab(tab_id='tab2')
        assert tab is not None
        assert tab.title == 'Child Tab'

    def test_tab_by_title(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        tab = doc.tab(title='Main')
        assert tab is not None
        assert tab.tab_id == 'tab1'

    def test_tab_not_found(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        assert doc.tab(tab_id='nonexistent') is None

    def test_repr(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        assert repr(doc) == '<Document "My Document">'


class TestDocumentTab:

    def test_text_extraction(self):
        tab = DocumentTab(
            {'tabId': 't1', 'title': 'Tab'},
            {},
            {'content': [
                {'paragraph': {'elements': [
                    {'textRun': {'content': 'Line 1\n'}},
                ]}},
                {'paragraph': {'elements': [
                    {'textRun': {'content': 'Line 2\n'}},
                ]}},
            ]}
        )
        assert tab.text == 'Line 1\nLine 2\n'

    def test_structural_elements(self):
        body = {'content': [{'paragraph': {}}, {'table': {}}]}
        tab = DocumentTab({'tabId': 't1'}, {}, body)
        assert len(tab.structural_elements) == 2

    def test_empty_body(self):
        tab = DocumentTab({'tabId': 't1'}, {}, None)
        assert tab.structural_elements == []
        assert tab.text == ''


class TestDocsReplaceAll:

    def test_replace_all(self, docs_service):
        replacements = {'{{name}}': 'Alice', '{{date}}': '2025-01-01'}

        with patch('driveclient.docs.execute_with_backoff', return_value={'replies': []}) as mock_exec:
            docs_service.replace_all('doc1', replacements)

        docs_service.service.documents().batchUpdate.assert_called_once()
        call_kwargs = docs_service.service.documents().batchUpdate.call_args[1]
        assert call_kwargs['documentId'] == 'doc1'
        requests = call_kwargs['body']['requests']
        assert len(requests) == 2
        texts = {r['replaceAllText']['containsText']['text'] for r in requests}
        assert texts == {'{{name}}', '{{date}}'}

    def test_replace_all_with_tab_ids(self, docs_service):
        with patch('driveclient.docs.execute_with_backoff', return_value={'replies': []}):
            docs_service.replace_all('doc1', {'old': 'new'}, tab_ids=['tab1'])

        call_kwargs = docs_service.service.documents().batchUpdate.call_args[1]
        req = call_kwargs['body']['requests'][0]
        assert req['replaceAllText']['tabsCriteria']['tabIds'] == ['tab1']


class TestDocsCopyWithStyles:

    def test_copy_with_styles(self, docs_service):
        from driveclient import DriveFile
        mock_file = DriveFile(MagicMock(), {'id': 'copy1', 'mimeType': 'text/plain', 'name': 'x'})
        docs_service.drive_client.copy.return_value = mock_file

        with patch('driveclient.docs.execute_with_backoff', return_value=SAMPLE_DOC_DATA):
            result = docs_service.copy_with_styles('src1', name='My Copy', parent_id='fld1')

        docs_service.drive_client.copy.assert_called_once_with(
            'src1', name='My Copy', parent_id='fld1'
        )
        assert isinstance(result, Document)

    def test_copy_without_drive_client_raises(self):
        svc = DocsService(MagicMock(), drive_client=None)
        with pytest.raises(RuntimeError, match='requires a DriveClient'):
            svc.copy_with_styles('src1')


class TestDocsMerge:

    def test_merge_appends_content(self, docs_service):
        with patch('driveclient.docs.execute_with_backoff') as mock_exec:
            mock_exec.side_effect = [
                SAMPLE_DOC_DATA,  # get source
                {'replies': []},  # batchUpdate
            ]
            docs_service.merge('target1', 'source1')

        batch_call = docs_service.service.documents().batchUpdate
        assert batch_call.called
        body = batch_call.call_args[1]['body']
        requests = body['requests']
        assert any('insertText' in r for r in requests)

    def test_merge_requests_have_no_tab_id(self, docs_service):
        with patch('driveclient.docs.execute_with_backoff') as mock_exec:
            mock_exec.side_effect = [SAMPLE_DOC_DATA, {'replies': []}]
            docs_service.merge('target1', 'source1')

        body = docs_service.service.documents().batchUpdate.call_args[1]['body']
        for req in body['requests']:
            if 'insertText' in req:
                location = req['insertText']['endOfSegmentLocation']
                assert 'tabId' not in location


class TestUtf16Len:

    def test_ascii(self):
        assert _utf16_len('hello') == 5

    def test_empty(self):
        assert _utf16_len('') == 0

    def test_emoji_surrogate_pair(self):
        assert _utf16_len('\U0001F600') == 2  # grinning face

    def test_bmp_unicode(self):
        assert _utf16_len('\u00e9') == 1  # e with acute


class TestBuildTextStyleFields:

    def test_basic_fields(self):
        style = {'bold': True, 'italic': True, 'fontSize': {'magnitude': 12, 'unit': 'PT'}}
        result = _build_text_style_fields(style)
        assert result == 'bold,fontSize,italic'

    def test_empty_style(self):
        assert _build_text_style_fields({}) == ''
        assert _build_text_style_fields(None) == ''

    def test_unknown_keys_ignored(self):
        style = {'bold': True, 'headingId': 'h1', 'unknownField': 'x'}
        assert _build_text_style_fields(style) == 'bold'


class TestBuildParagraphStyleFields:

    def test_named_style_type(self):
        style = {'namedStyleType': 'HEADING_1', 'alignment': 'CENTER'}
        result = _build_paragraph_style_fields(style)
        assert result == 'alignment,namedStyleType'

    def test_read_only_keys_excluded(self):
        style = {'namedStyleType': 'NORMAL_TEXT', 'headingId': 'h.abc'}
        assert _build_paragraph_style_fields(style) == 'namedStyleType'


class TestResolveBulletPreset:

    def test_ordered_list(self):
        lists = {
            'list1': {
                'listProperties': {
                    'nestingLevels': [{'glyphType': 'DECIMAL'}]
                }
            }
        }
        assert _resolve_bullet_preset('list1', lists) == 'NUMBERED_DECIMAL_ALPHA_ROMAN'

    def test_unordered_list(self):
        lists = {
            'list1': {
                'listProperties': {
                    'nestingLevels': [{'glyphType': 'DISC'}]
                }
            }
        }
        assert _resolve_bullet_preset('list1', lists) == 'BULLET_DISC_CIRCLE_SQUARE'

    def test_missing_list(self):
        assert _resolve_bullet_preset('missing', {'other': {}}) == 'BULLET_DISC_CIRCLE_SQUARE'

    def test_none_lists(self):
        assert _resolve_bullet_preset('list1', None) == 'BULLET_DISC_CIRCLE_SQUARE'

    def test_none_list_id(self):
        assert _resolve_bullet_preset(None, {'list1': {}}) == 'BULLET_DISC_CIRCLE_SQUARE'


class TestBuildCopyRequests:

    def _make_tab(self, content):
        return DocumentTab({'tabId': 't1'}, {}, {'content': content})

    def test_basic_text(self):
        tab = self._make_tab([
            {'paragraph': {'elements': [{'textRun': {'content': 'Hello\n'}}]}}
        ])
        requests = _build_copy_requests(tab)
        assert len(requests) == 1
        assert requests[0]['insertText']['text'] == 'Hello\n'

    def test_with_tab_id(self):
        tab = self._make_tab([
            {'paragraph': {'elements': [{'textRun': {'content': 'Hi\n'}}]}}
        ])
        requests = _build_copy_requests(tab, target_tab_id='target_tab')
        insert = requests[0]
        assert insert['insertText']['endOfSegmentLocation']['tabId'] == 'target_tab'

    def test_text_style_applied_with_correct_ranges(self):
        tab = self._make_tab([
            {'paragraph': {'elements': [
                {'textRun': {'content': 'Bold', 'textStyle': {'bold': True}}},
                {'textRun': {'content': ' text\n'}},
            ]}}
        ])
        requests = _build_copy_requests(tab)
        inserts = [r for r in requests if 'insertText' in r]
        styles = [r for r in requests if 'updateTextStyle' in r]
        assert len(inserts) == 2
        assert len(styles) == 1
        style_req = styles[0]['updateTextStyle']
        assert style_req['range']['startIndex'] == 1
        assert style_req['range']['endIndex'] == 5  # "Bold" = 4 chars, start at 1
        assert style_req['fields'] == 'bold'

    def test_paragraph_style(self):
        tab = self._make_tab([
            {'paragraph': {
                'paragraphStyle': {'namedStyleType': 'HEADING_1'},
                'elements': [{'textRun': {'content': 'Title\n'}}],
            }}
        ])
        requests = _build_copy_requests(tab)
        para_styles = [r for r in requests if 'updateParagraphStyle' in r]
        assert len(para_styles) == 1
        assert para_styles[0]['updateParagraphStyle']['paragraphStyle']['namedStyleType'] == 'HEADING_1'

    def test_bullets(self):
        lists = {
            'list1': {
                'listProperties': {
                    'nestingLevels': [{'glyphType': 'DECIMAL'}]
                }
            }
        }
        tab = self._make_tab([
            {'paragraph': {
                'bullet': {'listId': 'list1', 'nestingLevel': 0},
                'elements': [{'textRun': {'content': 'Item\n'}}],
            }}
        ])
        requests = _build_copy_requests(tab, lists=lists)
        bullet_reqs = [r for r in requests if 'createParagraphBullets' in r]
        assert len(bullet_reqs) == 1
        assert bullet_reqs[0]['createParagraphBullets']['bulletPreset'] == 'NUMBERED_DECIMAL_ALPHA_ROMAN'

    def test_skips_non_paragraph_elements(self):
        tab = self._make_tab([
            {'table': {'rows': 1, 'columns': 1}},
            {'sectionBreak': {}},
            {'paragraph': {'elements': [{'textRun': {'content': 'Text\n'}}]}},
        ])
        requests = _build_copy_requests(tab)
        inserts = [r for r in requests if 'insertText' in r]
        assert len(inserts) == 1

    def test_multiple_runs_per_paragraph(self):
        tab = self._make_tab([
            {'paragraph': {'elements': [
                {'textRun': {'content': 'aaa'}},
                {'textRun': {'content': 'bbb\n'}},
            ]}}
        ])
        requests = _build_copy_requests(tab)
        inserts = [r for r in requests if 'insertText' in r]
        assert len(inserts) == 2
        assert inserts[0]['insertText']['text'] == 'aaa'
        assert inserts[1]['insertText']['text'] == 'bbb\n'

    def test_empty_tab(self):
        tab = self._make_tab([])
        assert _build_copy_requests(tab) == []

    def test_request_ordering_inserts_before_styles(self):
        tab = self._make_tab([
            {'paragraph': {'elements': [
                {'textRun': {'content': 'Styled\n', 'textStyle': {'bold': True}}},
            ]}}
        ])
        requests = _build_copy_requests(tab)
        insert_indices = [i for i, r in enumerate(requests) if 'insertText' in r]
        style_indices = [i for i, r in enumerate(requests) if 'updateTextStyle' in r]
        assert max(insert_indices) < min(style_indices)


class TestDocumentAddTab:

    def test_returns_tab_id(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        reply = {
            'replies': [{
                'addDocumentTab': {
                    'tabProperties': {'tabId': 'new_tab_123'}
                }
            }]
        }
        with patch('driveclient.docs.execute_with_backoff', return_value=reply):
            tab_id = doc.add_tab('Spanish')

        assert tab_id == 'new_tab_123'

    def test_passes_index(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        reply = {
            'replies': [{
                'addDocumentTab': {
                    'tabProperties': {'tabId': 'new_tab'}
                }
            }]
        }
        with patch('driveclient.docs.execute_with_backoff', return_value=reply):
            doc.add_tab('French', index=2)

        call_kwargs = docs_service.service.documents().batchUpdate.call_args[1]
        req = call_kwargs['body']['requests'][0]
        assert req['addDocumentTab']['tabProperties']['index'] == 2

    def test_invalidates_cache(self, docs_service):
        doc = Document(docs_service, 'doc1', SAMPLE_DOC_DATA)
        assert doc._data is not None
        reply = {
            'replies': [{
                'addDocumentTab': {
                    'tabProperties': {'tabId': 'new_tab'}
                }
            }]
        }
        with patch('driveclient.docs.execute_with_backoff', return_value=reply):
            doc.add_tab('German')
        assert doc._data is None


class TestDocsCopyToTab:

    def test_fetches_source_and_sends_batch_update(self, docs_service):
        with patch('driveclient.docs.execute_with_backoff') as mock_exec:
            mock_exec.side_effect = [
                SAMPLE_DOC_DATA,    # get source
                {'replies': []},    # batchUpdate on target
            ]
            docs_service.copy_to_tab('src_doc', 'tgt_doc', 'tgt_tab')

        batch_call = docs_service.service.documents().batchUpdate
        assert batch_call.called
        call_kwargs = batch_call.call_args[1]
        assert call_kwargs['documentId'] == 'tgt_doc'
        requests = call_kwargs['body']['requests']
        inserts = [r for r in requests if 'insertText' in r]
        assert len(inserts) > 0
        for ins in inserts:
            assert ins['insertText']['endOfSegmentLocation']['tabId'] == 'tgt_tab'


class TestDocumentImportDocument:

    def test_creates_tab_and_copies_content(self, docs_service):
        doc = Document(docs_service, 'target_doc', SAMPLE_DOC_DATA)

        add_tab_reply = {
            'replies': [{
                'addDocumentTab': {
                    'tabProperties': {'tabId': 'imported_tab'}
                }
            }]
        }
        with patch('driveclient.docs.execute_with_backoff') as mock_exec:
            mock_exec.side_effect = [
                add_tab_reply,      # add_tab batch_update
                SAMPLE_DOC_DATA,    # copy_to_tab: get source
                {'replies': []},    # copy_to_tab: batchUpdate
            ]
            tab_id = doc.import_document('source_doc', 'Spanish')

        assert tab_id == 'imported_tab'
