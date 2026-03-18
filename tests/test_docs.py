"""Tests for Google Docs API integration."""

from unittest.mock import MagicMock, patch

import pytest

from driveclient.docs import DocsService, Document, DocumentTab


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
            # First call: get source doc
            mock_exec.side_effect = [
                SAMPLE_DOC_DATA,  # get source
                {'replies': []},  # batchUpdate
            ]
            docs_service.merge('target1', 'source1')

        # batchUpdate should have been called with insertText requests
        batch_call = docs_service.service.documents().batchUpdate
        assert batch_call.called
        body = batch_call.call_args[1]['body']
        requests = body['requests']
        assert any('insertText' in r for r in requests)
