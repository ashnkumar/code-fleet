"""Base62 encoding for short codes."""

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE = len(ALPHABET)


def encode(n: int) -> str:
    """Encode a non-negative integer as a base62 string."""
    if n < 0:
        raise ValueError("n must be non-negative")
    if n == 0:
        return ALPHABET[0]
    out = []
    while n:
        n, rem = divmod(n, BASE)
        out.append(ALPHABET[rem])
    return "".join(reversed(out))


def decode(code: str) -> int:
    """Decode a base62 string back to an integer."""
    if not code:
        raise ValueError("code must not be empty")
    n = 0
    for ch in code:
        try:
            n = n * BASE + ALPHABET.index(ch)
        except ValueError as exc:
            raise ValueError(f"invalid base62 character: {ch!r}") from exc
    return n
