"""Tests for utility functions."""

import hashlib
import time
from unittest.mock import MagicMock, patch

import pytest

from googleapiclient.errors import HttpError

from driveclient.utils import execute_with_backoff, hashfile


class TestExecuteWithBackoff:

    def test_success_on_first_try(self):
        request = MagicMock()
        request.execute.return_value = {'id': '123'}

        result = execute_with_backoff(request)

        assert result == {'id': '123'}
        request.execute.assert_called_once()

    def test_retries_on_rate_limit(self):
        request = MagicMock()
        error = HttpError(
            MagicMock(status=429, reason='Rate Limit Exceeded'),
            b'rate limit exceeded'
        )
        request.execute.side_effect = [error, {'id': '123'}]

        with patch('driveclient.utils.time.sleep'):
            result = execute_with_backoff(request, max_retries=3)

        assert result == {'id': '123'}
        assert request.execute.call_count == 2

    def test_returns_none_on_not_found(self):
        request = MagicMock()
        error = HttpError(
            MagicMock(status=404, reason='Not Found'),
            b'not found'
        )
        request.execute.side_effect = error

        result = execute_with_backoff(request)

        assert result is None

    def test_returns_none_on_invalid_change(self):
        request = MagicMock()
        error = HttpError(
            MagicMock(status=400, reason='Invalid Change'),
            b'invalid change'
        )
        request.execute.side_effect = error

        result = execute_with_backoff(request)

        assert result is None

    def test_raises_other_errors(self):
        request = MagicMock()
        error = HttpError(
            MagicMock(status=403, reason='Forbidden'),
            b'forbidden'
        )
        request.execute.side_effect = error

        with pytest.raises(HttpError):
            execute_with_backoff(request)

    def test_returns_none_after_max_retries(self):
        request = MagicMock()
        error = HttpError(
            MagicMock(status=429, reason='Rate Limit Exceeded'),
            b'rate limit exceeded'
        )
        request.execute.side_effect = error

        with patch('driveclient.utils.time.sleep'):
            result = execute_with_backoff(request, max_retries=2)

        assert result is None
        assert request.execute.call_count == 2


class TestHashfile:

    def test_sha1_default(self, tmp_path):
        path = tmp_path / 'test.bin'
        path.write_bytes(b'hello world')

        result = hashfile(str(path))

        expected = hashlib.sha1(b'hello world').hexdigest()
        assert result == expected

    def test_md5(self, tmp_path):
        path = tmp_path / 'test.bin'
        path.write_bytes(b'hello world')

        result = hashfile(str(path), hasher=hashlib.md5())

        expected = hashlib.md5(b'hello world').hexdigest()
        assert result == expected

    def test_large_file(self, tmp_path):
        """Verify hashing works with multiple blocks."""
        path = tmp_path / 'large.bin'
        data = b'x' * (2**16 + 100)  # slightly over one block
        path.write_bytes(data)

        result = hashfile(str(path))

        expected = hashlib.sha1(data).hexdigest()
        assert result == expected
