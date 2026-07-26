"""Payload canonicalization + hashing.

WRC injects two server-generated HTML comments right before ``</html>`` that
drift between fetches even when the decision text is unchanged. If we stored
the raw response bytes, ``sha256(stored_bytes)`` would change on every re-run
and task req 9 ("Use the file hash to detect changes between runs") would
fire a false "changed" on every record forever.

The fix is to **canonicalize before storing**: strip the volatile markers up
front, write the canonical bytes to MinIO, and hash the canonical bytes.
Result — one hash, one payload, ``sha256(<object-in-minio>) == file_hash`` in
Mongo, and re-runs produce byte-identical objects for unchanged decisions.

Known volatile markers (extend the list as new ones surface):

  1. ``<!-- Elapsed time: 0.0625034 -->`` — always present; the trailing
     number is server render time and varies per request.
  2. ``<!-- cached or not being index.aspx page -->`` — appears only on
     cache-cold responses; disappears on the warm re-fetch.

PDF/DOC payloads have no server-injected volatility, so callers pass them
through un-canonicalized.
"""

from __future__ import annotations

import hashlib
import re

# One entry per known volatile marker. Order-independent; substitutions are
# idempotent, so re-running the whole list is cheap.
_VOLATILE_HTML_MARKERS: tuple[re.Pattern[bytes], ...] = (
    re.compile(rb"<!--\s*Elapsed time:[^>]*?-->", re.DOTALL),
    re.compile(rb"<!--\s*cached or not being index\.aspx page\s*-->", re.DOTALL),
)


def sha256_hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonicalize_html(payload: bytes) -> bytes:
    """Return ``payload`` with all known volatile server markers removed.

    Idempotent — calling twice yields the same bytes. Callers should apply
    this once, right before the MinIO put, so the stored object and the
    computed ``file_hash`` are always in sync.
    """
    for pattern in _VOLATILE_HTML_MARKERS:
        payload = pattern.sub(b"", payload)
    return payload
