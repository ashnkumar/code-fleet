"""Configuration for linkstash.

Currently a flat set of module-level constants. Anything that imports these
should go through this module rather than hardcoding values.
"""

# Base URL that short codes are appended to.
BASE_URL = "https://lnk.st/"

# Reject URLs longer than this.
MAX_URL_LENGTH = 2048

# Codes are allocated from this offset so the first one is not "0".
CODE_OFFSET = 100_000

# Maximum requests allowed per client per minute.
RATE_LIMIT_PER_MINUTE = 60
