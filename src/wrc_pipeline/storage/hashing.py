"""Content hashing helpers.

Two hashes, two purposes (see decisions.md §4.3):

* ``sha256_hash`` — SHA-256 of the exact stored bytes. Satisfies task req 8
  ("calculate the file_hash of the file and store it"). This is what a reviewer
  can independently reproduce by hashing the object in MinIO.

* ``content_fingerprint`` — change-detection key. WRC injects two volatile
  server-generated HTML comments right before ``</html>`` that drift between
  fetches even when the decision text is unchanged; hashing the raw bytes
  would report "changed" on every re-run and defeat req 9. The fingerprint
  strips only these confirmed-volatile markers; every other comment is
  preserved in case it carries real content.

  Known volatile markers (extend the list as new ones surface):

    1. ``<!-- Elapsed time: 0.0625034 -->`` — always present; the trailing
       number is server render time and varies per request.
    2. ``<!-- cached or not being index.aspx page -->`` — appears only on
       cache-cold responses; disappears on the warm re-fetch.

  For PDF/DOC payloads there is no server-injected volatility, so the
  fingerprint collapses to the raw ``sha256_hash``.
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


def content_fingerprint(payload: bytes, ext: str) -> str:
    if ext == "html":
        for pattern in _VOLATILE_HTML_MARKERS:
            payload = pattern.sub(b"", payload)
    return hashlib.sha256(payload).hexdigest()
