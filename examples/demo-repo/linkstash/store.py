"""In-memory link store."""

from linkstash import config
from linkstash.codec import decode, encode


class LinkStore:
    """Maps short codes to URLs.

    Codes are allocated sequentially from ``config.CODE_OFFSET`` and encoded
    with base62, so they stay short and opaque.
    """

    def __init__(self) -> None:
        self._urls: dict[int, str] = {}
        self._next_id = config.CODE_OFFSET

    def save(self, url: str) -> str:
        """Store a URL and return its short code."""
        if not url:
            raise ValueError("url must not be empty")
        if len(url) > config.MAX_URL_LENGTH:
            raise ValueError("url too long")
        link_id = self._next_id
        self._next_id += 1
        self._urls[link_id] = url
        return encode(link_id)

    def resolve(self, code: str) -> str | None:
        """Return the URL for a short code, or None if unknown."""
        try:
            link_id = decode(code)
        except ValueError:
            return None
        return self._urls.get(link_id)

    def short_url(self, code: str) -> str:
        """Return the full short URL for a code."""
        return config.BASE_URL + code

    def __len__(self) -> int:
        return len(self._urls)
