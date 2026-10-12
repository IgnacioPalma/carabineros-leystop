"""Polite HTTP client for leystop.carabineros.cl.

The site sits behind a WAF that answers with a CAPTCHA page once it flags a
client IP (e.g. after requests to unknown URLs or bursts of parallel requests).
This client therefore:

* only requests one URL at a time, with a minimum gap between requests;
* never retries a CAPTCHA/validation page — it raises `Blocked` so the caller
  can checkpoint and stop; we do not try to get around the WAF;
* retries only plain transient failures (timeouts, 5xx, 429) with backoff.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

BASE_URL = "https://leystop.carabineros.cl"
REFERER = f"{BASE_URL}/estadistica/registros"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; leystop-open-data/0.1; "
    "+https://github.com/IgnacioPalma/carabineros-leystop)"
)

log = logging.getLogger(__name__)


class Blocked(RuntimeError):
    """The WAF is challenging us (CAPTCHA). Stop and try again later."""


def _looks_blocked(text: str) -> bool:
    t = text[:4000].lower()
    return "captcha" in t or "validation request" in t or "user validation required" in t


class Client:
    def __init__(
        self,
        *,
        min_interval: float = 1.5,
        timeout: float = 120.0,
        max_retries: int = 5,
        user_agent: str = DEFAULT_USER_AGENT,
        proxy: str | None = None,
        min_gap: float = 3.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.min_interval = min_interval
        self.min_gap = min_gap
        self.max_retries = max_retries
        self._last = 0.0
        self._done = 0.0
        self.requests = 0
        self._http = httpx.Client(
            base_url=BASE_URL,
            timeout=timeout,
            # carabineros.cl drops connections from non-Chilean / cloud IPs, so CI
            # needs a proxy in Chile. Only this client uses it (R2 traffic does not).
            proxy=proxy,
            trust_env=False,
            transport=transport,
            follow_redirects=True,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "es-CL,es;q=0.9",
                "Referer": REFERER,
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        self._warmed_up = False

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        wait = max(self._last + self.min_interval, self._done + self.min_gap) - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def _warm_up(self) -> None:
        # Load the public page once, like a browser would, to get session cookies.
        self._throttle()
        try:
            r = self._http.get("/estadistica/registros", headers={"Accept": "text/html"})
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            raise ConnectionError(
                f"cannot reach {BASE_URL} ({e!r}). The site blocks non-Chilean IPs; "
                "run from Chile or set LEYSTOP_PROXY to a proxy in Chile."
            ) from e
        self.requests += 1
        self._done = time.monotonic()
        if _looks_blocked(r.text):
            raise Blocked("WAF challenge on landing page")
        self._warmed_up = True

    def get_json(self, path: str) -> Any:
        if not self._warmed_up:
            self._warm_up()
        delay = 5.0
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            try:
                r = self._http.get(path)
                self.requests += 1
            except httpx.TransportError as e:
                log.warning("GET %s failed (%s), attempt %d", path, e, attempt)
            else:
                if _looks_blocked(r.text):
                    raise Blocked(f"WAF challenge on {path}")
                if r.status_code == 200:
                    try:
                        return r.json()
                    except ValueError:
                        # Non-JSON 200 from the WAF/proxy: treat as transient.
                        log.warning("GET %s returned non-JSON body, attempt %d", path, attempt)
                elif r.status_code == 429 or r.status_code >= 500:
                    log.warning("GET %s -> HTTP %d, attempt %d", path, r.status_code, attempt)
                else:
                    # 404 (the WAF's own "URL cannot be found" page) or other 4xx on a
                    # URL the web app itself uses: the WAF's rate control has kicked in
                    # and the next step is a CAPTCHA. Stop and cool down; don't retry.
                    raise Blocked(f"GET {path} -> HTTP {r.status_code}: {r.text[:100]!r}")
            finally:
                self._done = time.monotonic()
            time.sleep(delay)
            delay = min(delay * 2, 120)
        raise RuntimeError(f"GET {path} failed after {self.max_retries} attempts")
