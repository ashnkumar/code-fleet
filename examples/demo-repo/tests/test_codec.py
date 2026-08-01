from linkstash.codec import decode, encode

import pytest


def test_zero_round_trips():
    assert encode(0) == "0"
    assert decode("0") == 0


@pytest.mark.parametrize("n", [1, 61, 62, 63, 100_000, 999_999_999])
def test_round_trip(n):
    assert decode(encode(n)) == n


def test_negative_rejected():
    with pytest.raises(ValueError):
        encode(-1)


def test_invalid_character_rejected():
    with pytest.raises(ValueError):
        decode("abc!")


def test_empty_rejected():
    with pytest.raises(ValueError):
        decode("")
