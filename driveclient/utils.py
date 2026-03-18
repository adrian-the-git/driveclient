"""
Shared utilities: exponential backoff execution, file hashing, debug logging.
"""

import hashlib
import os
import random
import time
from functools import partial
from pprint import pprint
from urllib.parse import parse_qs, urlparse

from googleapiclient.errors import HttpError

DEBUG = 'DRIVECLIENT_DEBUG' in os.environ


def debug_log(*args):
    """Print a debug message if DRIVECLIENT_DEBUG is set."""
    if DEBUG:
        print('driveclient:', *args)


def execute_with_backoff(request, max_retries=10):
    """
    Execute a Google API request with exponential backoff on rate limits.
    Returns None for not-found or invalid-change errors.
    """
    DEBUG and _dump_request(request)

    for i in range(max_retries):
        try:
            return request.execute()
        except HttpError as error:
            reason = error._get_reason().lower().replace(' ', '')
            if 'ratelimitexceeded' in reason:
                time.sleep(2**i + random.random())
                continue
            elif 'notfound' in reason:
                return None
            elif 'invalidchange' in reason:
                return None
            raise
    return None


def hashfile(filename, hasher=None, blocksize=2**16):
    """Hash a file without reading the entire thing into memory."""
    hasher = hasher or hashlib.sha1()
    with open(filename, 'rb') as f:
        for block in iter(partial(f.read, blocksize), b''):
            hasher.update(block)
        return hasher.hexdigest()


def _dump_request(request):
    """Print debug information about an API request."""
    print('driveclient:', request.methodId)
    print(request.method, request.uri)
    if request.method == 'GET':
        pprint(parse_qs(urlparse(request.uri).query))
    elif request.method in ('PUT', 'POST'):
        pprint(request.body)
    print()
