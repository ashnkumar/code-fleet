from linkstash import config
from linkstash.store import LinkStore

import pytest


def test_save_and_resolve():
    store = LinkStore()
    code = store.save("https://example.com/a")
    assert store.resolve(code) == "https://example.com/a"


def test_codes_are_distinct():
    store = LinkStore()
    codes = {store.save(f"https://example.com/{i}") for i in range(20)}
    assert len(codes) == 20


def test_unknown_code_returns_none():
    assert LinkStore().resolve("zzzz") is None


def test_malformed_code_returns_none():
    assert LinkStore().resolve("!!") is None


def test_empty_url_rejected():
    with pytest.raises(ValueError):
        LinkStore().save("")


def test_overlong_url_rejected():
    with pytest.raises(ValueError):
        LinkStore().save("https://example.com/" + "x" * config.MAX_URL_LENGTH)


def test_short_url_uses_base_url():
    store = LinkStore()
    code = store.save("https://example.com")
    assert store.short_url(code) == config.BASE_URL + code
