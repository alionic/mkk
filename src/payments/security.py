"""DNS-name and URL policy; DNS resolution belongs to the outbound transport."""

import re
import socket
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

import idna

if TYPE_CHECKING:
    from payments.config import Settings


def normalize_domain(value: str) -> str:
    """Return an IDNA ASCII DNS name, never a wildcard, URL, or IP literal."""
    if not value or value != value.strip() or any(char.isspace() for char in value):
        raise ValueError("invalid webhook domain")
    try:
        domain = idna.encode(value, uts46=True, std3_rules=True).decode("ascii").lower()
    except idna.IDNAError:
        raise ValueError("invalid webhook domain") from None
    # Remove one root dot after UTS-46 has normalized Unicode dot variants.
    # Empty labels (including doubled root dots) were rejected by IDNA above.
    domain = domain.removesuffix(".")
    if not domain or len(domain) > 253:
        raise ValueError("invalid webhook domain")
    try:
        # Pure parsing, no DNS/I/O; unlike ip_address this also recognizes
        # historical dotted hexadecimal/octal and shortened IPv4 spellings.
        socket.inet_aton(domain)
    except OSError:
        pass
    else:
        raise ValueError("webhook IP literals are forbidden")
    # Reject legacy numeric IPv4 spellings as well as purely numeric TLDs.
    if domain.rsplit(".", 1)[-1].isdigit():
        raise ValueError("webhook IP literals are forbidden")
    return domain


def normalize_webhook_url(url: str, *, allow_unsafe: bool = False) -> str:
    """Validate syntax and normalize the host without resolving DNS.

    Unsafe mode only relaxes scheme/port policy for development. It does not
    permit credentials, fragments, malformed URLs, or literal IP addresses.
    """
    if (
        not url
        or len(url) > 2048
        or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in url)
        or "\\" in url
        or "#" in url
        or re.search(r"%0[ad]", url, re.IGNORECASE)
    ):
        raise ValueError("invalid webhook URL")
    try:
        parts = urlsplit(url)
        host = parts.hostname
        port = parts.port
    except ValueError:
        raise ValueError("invalid webhook URL") from None
    if not host or parts.username is not None or parts.password is not None:
        raise ValueError("invalid webhook URL authority")
    if parts.netloc.endswith(":") or port == 0:
        raise ValueError("invalid webhook URL port")
    schemes = {"https", "http"} if allow_unsafe else {"https"}
    if parts.scheme not in schemes or (not allow_unsafe and port not in (None, 443)):
        raise ValueError("webhook requires HTTPS on port 443")
    host = normalize_domain(host)
    default_port = 443 if parts.scheme == "https" else 80
    authority = host if port in (None, default_port) else f"{host}:{port}"
    normalized = urlunsplit((parts.scheme, authority, parts.path, parts.query, ""))
    if len(normalized) > 2048:
        raise ValueError("webhook URL is too long")
    return normalized


def validate_webhook_url(url: str, settings: "Settings") -> str:
    """Enforce the configured domain boundary and return the normalized URL.

    This is not an SSRF-safe HTTP transport: the connector must additionally
    validate every resolved address at connection time, including on retries.
    """
    normalized = normalize_webhook_url(url, allow_unsafe=settings.allow_unsafe_webhooks)
    host = urlsplit(normalized).hostname
    if host is None or not any(
        host == domain or host.endswith("." + domain) for domain in settings.webhook_allowed_domains
    ):
        raise ValueError("webhook domain is not allowed")
    return normalized
