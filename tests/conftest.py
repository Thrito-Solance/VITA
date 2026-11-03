"""Parametrize over InMemoryBackend + Redis. Refuse to flush non-local Redis."""
# SPDX-License-Identifier: AGPL-3.0-or-later
# Copyright (C) 2026 thrito-solance
from __future__ import annotations

import ipaddress
import os
import uuid
from urllib.parse import urlparse

import pytest

from vita.persistence import InMemoryBackend


def pytest_addoption(parser):
    parser.addoption("--redis-url", default=os.environ.get("REDIS_URL", ""))


def _assert_safe_test_target(url: str) -> None:
    host = urlparse(url).hostname or ""
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        if host not in ("localhost", "redis", ""):
            raise SystemExit(f"REFUSING non-local Redis test target: host={host!r}")
        return
    if not (addr.is_loopback or host in ("redis",)):
        raise SystemExit(f"REFUSING non-loopback Redis test target: {addr}")


class NamespacedRedisBackend:
    def __init__(self, client, prefix: str) -> None:
        self._client = client
        self._prefix = prefix

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def set(self, key, value, *, nx=False, ex=None):
        return self._client.set(self._key(key), value, nx=nx, ex=ex)

    def get(self, key):
        return self._client.get(self._key(key))

    def delete(self, *keys):
        return self._client.delete(*(self._key(key) for key in keys))

    def exists(self, *keys):
        return self._client.exists(*(self._key(key) for key in keys))

    def xadd(self, name, fields, *, maxlen=None):
        return self._client.xadd(self._key(name), fields, maxlen=maxlen)

    def xrange(self, name, start="-", end="+", count=None):
        return self._client.xrange(self._key(name), start, end, count)

    def eval(self, script, numkeys, *keys_and_args):
        keys = [self._key(key) for key in keys_and_args[:numkeys]]
        return self._client.eval(script, numkeys, *keys, *keys_and_args[numkeys:])


@pytest.fixture
def memory_backend() -> InMemoryBackend:
    return InMemoryBackend()


@pytest.fixture
def redis_backend(request):
    url = request.config.getoption("--redis-url")
    if not url:
        pytest.skip("--redis-url / REDIS_URL not set")
    _assert_safe_test_target(url)
    import redis as redis_lib
    client = redis_lib.from_url(url, decode_responses=True, socket_timeout=2.0)
    prefix = f"vita:test:{uuid.uuid4().hex}:"
    try:
        yield NamespacedRedisBackend(client, prefix)
    finally:
        keys = list(client.scan_iter(match=f"{prefix}*"))
        if keys:
            client.delete(*keys)
        client.close()


@pytest.fixture(params=["memory", "redis"])
def backend(request):
    if request.param == "memory":
        return request.getfixturevalue("memory_backend")
    return request.getfixturevalue("redis_backend")
