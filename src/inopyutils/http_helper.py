from __future__ import annotations

import asyncio
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple, Union

import aiohttp

class InoHttpHelper:
    """
    Async HTTP helper built on top of aiohttp.

    Features
    - Configurable timeouts, connection limits, retries, and default headers
    - Convenience async methods: get, post, put, delete, patch
    - Optional authentication (default at session level or per-request override) using aiohttp.BasicAuth
    - Automatic retry with exponential backoff on transient errors and 5xx responses
    - Usable as an async context manager or managed manually (close())

    Notes on return value
    - Each verb method returns a dict with at least the following keys:
        - success: bool
        - msg: str (error message or response reason)
        - status_code: int | None
        - headers: dict[str, str]
        - data: parsed body (JSON object, bytes, or text depending on flags)
        - url: final URL used
        - method: HTTP method
        - attempts: number of attempts used (including retries)
      You can force JSON parsing by passing json/json_response=True, regardless of content-type.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        *,
        # Timeouts (seconds)
        timeout_total: Optional[float] = 30.0,
        timeout_connect: Optional[float] = 10.0,
        timeout_sock_connect: Optional[float] = 10.0,
        timeout_sock_read: Optional[float] = 30.0,
        # Connection limits
        limit: Optional[int] = 100,
        limit_per_host: Optional[int] = 10,
        # Retry policy
        retries: int = 2,
        backoff_factor: float = 0.5,
        retry_for_statuses: Tuple[int, ...] = (429, 500, 502, 503, 504),
        # Defaults
        default_headers: Optional[Mapping[str, str]] = None,
        raise_for_status: bool = False,
        trust_env: bool = False,
        # Authentication
        auth: Optional[Union[aiohttp.BasicAuth, Tuple[str, str]]] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/") if base_url else None
        self._default_headers = dict(default_headers or {})
        self._raise_for_status = raise_for_status
        self._retries = max(0, retries)
        self._backoff_factor = max(0.0, backoff_factor)
        self._retry_for_statuses = set(retry_for_statuses)

        # Normalize default auth (supports passing (username, password))
        if isinstance(auth, tuple):
            self._auth: Optional[aiohttp.BasicAuth] = aiohttp.BasicAuth(auth[0], auth[1])
        else:
            self._auth = auth

        # Defer creating the actual aiohttp.ClientSession until we are inside a running event loop.
        self._timeout_params = dict(
            total=timeout_total,
            connect=timeout_connect,
            sock_connect=timeout_sock_connect,
            sock_read=timeout_sock_read,
        )
        self._connector_params = dict(limit=limit, limit_per_host=limit_per_host)
        self._trust_env = trust_env
        self._session: Optional[aiohttp.ClientSession] = None

    # Async context manager support
    async def __aenter__(self) -> "InoHttpHelper":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def _ensure_session(self) -> None:
        """
        Lazily create the aiohttp.ClientSession when an event loop is running.
        Safe to call multiple times.
        """
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(**self._timeout_params)
            connector = aiohttp.TCPConnector(**self._connector_params)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self._default_headers,
                trust_env=self._trust_env,
                auth=self._auth,
            )

    @property
    def session(self) -> aiohttp.ClientSession:
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    # Core request with retry
    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        json: Any = None,
        data: Any = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        return_bytes: bool = False,
        force_json: bool = False,
        allow_redirects: bool = True,
        auth: Optional[Union[aiohttp.BasicAuth, Tuple[str, str]]] = None,
    ) -> Dict[str, Any]:
        full_url = self._compose_url(url)
        merged_headers = self._merge_headers(headers)
        await self._ensure_session()
        # Normalize per-request auth override
        if isinstance(auth, tuple):
            auth_obj: Optional[aiohttp.BasicAuth] = aiohttp.BasicAuth(auth[0], auth[1])
        else:
            auth_obj = auth

        last_exc: Optional[BaseException] = None
        attempts = self._retries + 1
        for attempt in range(1, attempts + 1):
            try:
                async with self._session.request(
                    method.upper(),
                    full_url,
                    params=params,
                    headers=merged_headers,
                    json=json,
                    data=data,
                    timeout=timeout,
                    allow_redirects=allow_redirects,
                    auth=auth_obj,
                ) as resp:
                    if self._raise_for_status and resp.status >= 400:
                        # If raising, do not consume body first
                        resp.raise_for_status()

                    # Retry on specific statuses
                    if resp.status in self._retry_for_statuses and attempt < attempts:
                        await self._sleep_backoff(attempt)
                        continue

                    # Read body according to flags
                    content_type = resp.headers.get("Content-Type", "")
                    if force_json or ("json" in content_type.lower()):
                        body: Union[str, bytes, Any] = await resp.json(content_type=None)
                    elif return_bytes:
                        body = await resp.read()
                    else:
                        body = await resp.text()
                    # Convert headers to a plain dict for ease of use
                    headers_out = {k: v for k, v in resp.headers.items()}
                    success = resp.status < 400
                    result: Dict[str, Any] = {
                        "success": success,
                        "msg": resp.reason or "",
                        "status_code": resp.status,
                        "headers": headers_out,
                        "data": body,
                        "url": full_url,
                        "method": method.upper(),
                        "attempts": attempt,
                    }
                    return result

            except aiohttp.ClientResponseError as cre:
                # Retry on configured statuses already handled above; for explicit raise_for_status
                last_exc = cre
                if getattr(cre, "status", None) in self._retry_for_statuses and attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                # On last attempt or non-retryable status, return failure dict
                return {
                    "success": False,
                    "msg": str(cre),
                    "status_code": getattr(cre, "status", None),
                    "headers": {},
                    "data": None,
                    "url": full_url,
                    "method": method.upper(),
                    "attempts": attempt,
                }
            except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError, aiohttp.ClientOSError, aiohttp.TooManyRedirects) as ce:
                last_exc = ce
                if attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                return {
                    "success": False,
                    "msg": str(ce),
                    "status_code": None,
                    "headers": {},
                    "data": None,
                    "url": full_url,
                    "method": method.upper(),
                    "attempts": attempt,
                }
            except asyncio.TimeoutError as te:
                last_exc = te
                if attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                return {
                    "success": False,
                    "msg": "Request timed out: " + str(te),
                    "status_code": None,
                    "headers": {},
                    "data": None,
                    "url": full_url,
                    "method": method.upper(),
                    "attempts": attempt,
                }

        # Should not reach here; return failure dict if no response and no exception
        if last_exc:
            return {
                "success": False,
                "msg": str(last_exc),
                "status_code": getattr(last_exc, "status", None),
                "headers": {},
                "data": None,
                "url": full_url,
                "method": method.upper(),
                "attempts": attempts,
            }
        return {
            "success": False,
            "msg": "HTTP request failed without exception and without response",
            "status_code": None,
            "headers": {},
            "data": None,
            "url": full_url,
            "method": method.upper(),
            "attempts": attempts,
        }

    def _compose_url(self, url: str) -> str:
        if self._base_url and not url.lower().startswith(("http://", "https://")):
            return f"{self._base_url}/{url.lstrip('/')}"
        return url

    def _merge_headers(self, headers: Optional[Mapping[str, str]]) -> MutableMapping[str, str]:
        if not headers:
            return dict(self._default_headers)
        merged = dict(self._default_headers)
        merged.update(headers)
        return merged

    async def get(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        return_bytes: bool = False,
        json: bool = False,
        allow_redirects: bool = True,
        auth: Optional[Union[aiohttp.BasicAuth, Tuple[str, str]]] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "GET",
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            return_bytes=return_bytes,
            force_json=json,
            allow_redirects=allow_redirects,
            auth=auth,
        )

    async def post(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        json: Any = None,
        data: Any = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        return_bytes: bool = False,
        json_response: bool = False,
        allow_redirects: bool = True,
        auth: Optional[Union[aiohttp.BasicAuth, Tuple[str, str]]] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "POST",
            url,
            params=params,
            headers=headers,
            json=json,
            data=data,
            timeout=timeout,
            return_bytes=return_bytes,
            force_json=json_response,
            allow_redirects=allow_redirects,
            auth=auth,
        )

    async def put(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        json: Any = None,
        data: Any = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        return_bytes: bool = False,
        json_response: bool = False,
        allow_redirects: bool = True,
        auth: Optional[Union[aiohttp.BasicAuth, Tuple[str, str]]] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "PUT",
            url,
            params=params,
            headers=headers,
            json=json,
            data=data,
            timeout=timeout,
            return_bytes=return_bytes,
            force_json=json_response,
            allow_redirects=allow_redirects,
            auth=auth,
        )

    async def delete(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        return_bytes: bool = False,
        json: bool = False,
        allow_redirects: bool = True,
        auth: Optional[Union[aiohttp.BasicAuth, Tuple[str, str]]] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "DELETE",
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            return_bytes=return_bytes,
            force_json=json,
            allow_redirects=allow_redirects,
            auth=auth,
        )

    async def patch(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        json: Any = None,
        data: Any = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        return_bytes: bool = False,
        json_response: bool = False,
        allow_redirects: bool = True,
        auth: Optional[Union[aiohttp.BasicAuth, Tuple[str, str]]] = None,
    ) -> Dict[str, Any]:
        return await self._request(
            "PATCH",
            url,
            params=params,
            headers=headers,
            json=json,
            data=data,
            timeout=timeout,
            return_bytes=return_bytes,
            force_json=json_response,
            allow_redirects=allow_redirects,
            auth=auth,
        )

    async def _sleep_backoff(self, attempt: int) -> None:
        delay = self._backoff_factor * (2 ** (attempt - 1))
        # Add a small jitter to avoid thundering herd
        delay *= 1 + 0.1 * (attempt % 3)
        await asyncio.sleep(delay)
