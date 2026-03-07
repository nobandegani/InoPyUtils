from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple, Union, Callable

import aiohttp
import mimetypes
import re
from urllib.parse import urlparse, unquote

from .util_helper import InoUtilHelper, ino_ok, ino_err


def _sanitize_filename(name: str, fallback: str = "download") -> str:
    """Best-effort filename sanitization for cross-platform file systems."""
    if not name:
        return fallback
    candidate = Path(name).name.strip().strip("\0")
    candidate = candidate.replace("\\", "_").replace("/", "_")
    candidate = re.sub(r"[\x00-\x1f<>:\"|?*]", "_", candidate)
    candidate = candidate.rstrip(". ")
    if not candidate or candidate in {".", ".."}:
        return fallback
    return candidate


def _parse_total_from_content_range(content_range: str) -> Optional[int]:
    if not content_range or "/" not in content_range:
        return None
    total_part = content_range.split("/")[-1].strip()
    if total_part.isdigit():
        return int(total_part)
    return None

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
                    status = resp.status
                    extra = {
                        "status_code": status,
                        "headers": headers_out,
                        "data": body,
                        "url": full_url,
                        "method": method.upper(),
                        "attempts": attempt,
                    }
                    if status < 400:
                        return ino_ok(resp.reason or "", **extra)
                    else:
                        return ino_err(resp.reason or "", **extra)

            except aiohttp.ClientResponseError as cre:
                # Retry on configured statuses already handled above; for explicit raise_for_status
                last_exc = cre
                if getattr(cre, "status", None) in self._retry_for_statuses and attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                # On last attempt or non-retryable status, return failure dict
                return ino_err(
                    str(cre),
                    status_code=getattr(cre, "status", None),
                    headers={},
                    data=None,
                    url=full_url,
                    method=method.upper(),
                    attempts=attempt,
                )
            except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError, aiohttp.ClientOSError, aiohttp.TooManyRedirects) as ce:
                last_exc = ce
                if attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                return ino_err(
                    str(ce),
                    status_code=None,
                    headers={},
                    data=None,
                    url=full_url,
                    method=method.upper(),
                    attempts=attempt,
                )
            except asyncio.TimeoutError as te:
                last_exc = te
                if attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                return ino_err(
                    "Request timed out: " + str(te),
                    status_code=None,
                    headers={},
                    data=None,
                    url=full_url,
                    method=method.upper(),
                    attempts=attempt,
                )

        # Should not reach here; return failure dict if no response and no exception
        if last_exc:
            return ino_err(
                str(last_exc),
                status_code=getattr(last_exc, "status", None),
                headers={},
                data=None,
                url=full_url,
                method=method.upper(),
                attempts=attempts,
            )
        return ino_err(
            "HTTP request failed without exception and without response",
            status_code=None,
            headers={},
            data=None,
            url=full_url,
            method=method.upper(),
            attempts=attempts,
        )

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

    async def download(
        self,
        url: str,
        dest_path: Union[str, os.PathLike],
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        chunk_size: int = 1024  * 1024,
        overwrite: bool = False,
        resume: bool = True,
        progress: Optional[Callable[[int, Optional[int]], Any]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        allow_redirects: bool = True,
        auth: Optional[Union[aiohttp.BasicAuth, Tuple[str, str]]] = None,
        temp_suffix: str = ".part",
        mkdirs: bool = True,
        verify_size: bool = True,
        filename: Optional[str] = None,
        connection: int = 1,
    ) -> Dict[str, Any]:
        """
        Stream-download a file to disk without loading it into memory.

        Enhancements:
        - dest_path can be a directory. If it is, filename param or auto-derivation will determine the final file name.
        - Optional filename parameter to force the file name.
        - Auto-derives filename using Content-Disposition, final URL path, Content-Type, then fallback.

        - Resumable (HTTP Range) downloads when `resume=True` and a temp file exists.
        - Retries with exponential backoff like other helper methods.
        - Reports progress via a callback: progress(downloaded_bytes, total_bytes_or_None).
        - Atomic finalization: writes to temp file and renames to destination on success.

        Returns a dict like other methods with keys: success, msg, status_code, headers, data={"path", "bytes", "filename"}, url, method, attempts.
        """
        await self._ensure_session()
        full_url = self._compose_url(url)
        merged_headers = self._merge_headers(headers)

        # Normalize per-request auth override
        if isinstance(auth, tuple):
            auth_obj: Optional[aiohttp.BasicAuth] = aiohttp.BasicAuth(auth[0], auth[1])
        else:
            auth_obj = auth

        # Decide whether dest_path is a directory or a full file path
        dest_in = Path(dest_path)
        is_dir_hint = dest_in.exists() and dest_in.is_dir()
        if not is_dir_hint:
            text = str(dest_in)
            if text.endswith((os.sep, "/")) or (not dest_in.exists() and dest_in.suffix == ""):
                is_dir_hint = True

        # Base directory where the file should be saved
        base_dir = dest_in if is_dir_hint else dest_in.parent
        if mkdirs:
            base_dir.mkdir(parents=True, exist_ok=True)

        # Provisional filename decision before request (enables resume temp file)
        if filename:
            chosen_name = _sanitize_filename(filename)
        elif not is_dir_hint:
            chosen_name = _sanitize_filename(dest_in.name)
        else:
            # Try to get something from the URL path; fallback to 'download'
            url_name = Path(unquote(urlparse(full_url).path)).name

            unique_name = InoUtilHelper.hash_string(str(dest_path))

            chosen_name = _sanitize_filename(
                url_name if (url_name and "." in url_name and not url_name.startswith(".")) else unique_name,
                fallback=unique_name,
            )

        dest = base_dir / chosen_name
        tmp = dest.with_suffix(dest.suffix + temp_suffix)

        if dest.exists() and not overwrite:
            return ino_err(
                f"Destination exists and overwrite=False: {dest}",
                status_code=None,
                headers={},
                path="",
                bytes="",
                filename="",
                url=full_url,
                method="GET",
                attempts=0,
            )

        attempts = self._retries + 1
        try:
            requested_connections = int(connection)
        except (TypeError, ValueError):
            requested_connections = 1
        requested_connections = max(1, min(10, requested_connections))

        range_supported = False
        range_probe_total_size: Optional[int] = None
        if requested_connections > 1:
            range_supported, range_probe_total_size = await self._probe_range_support(
                full_url,
                params=params,
                headers=merged_headers,
                timeout=timeout,
                allow_redirects=allow_redirects,
                auth=auth_obj,
            )

        range_reset_retry_used = False
        last_exc: Optional[BaseException] = None
        attempt = 0
        while attempt < attempts:
            attempt += 1
            start_offset = tmp.stat().st_size if resume and tmp.exists() else 0
            # Set Range header if resuming and not already provided
            req_headers = dict(merged_headers)
            if start_offset > 0 and "Range" not in {k.title(): v for k, v in req_headers.items()}:
                req_headers["Range"] = f"bytes={start_offset}-"

            try:
                async with self._session.get(
                    full_url,
                    params=params,
                    headers=req_headers,
                    timeout=timeout,
                    allow_redirects=allow_redirects,
                    auth=auth_obj,
                ) as resp:
                    status = resp.status
                    if self._raise_for_status and status >= 400:
                        resp.raise_for_status()

                    if status < 200 or status >= 300:
                        # If resume offset became invalid (stale .part), drop partial and retry once from zero.
                        if status == 416 and start_offset > 0 and not range_reset_retry_used:
                            try:
                                tmp.unlink(missing_ok=True)
                            except Exception:
                                pass
                            range_reset_retry_used = True
                            attempt -= 1
                            continue

                        # For errors, do not persist response body to disk.
                        # Keep retries only for configured transient statuses.
                        if status in self._retry_for_statuses and attempt < attempts:
                            await self._sleep_backoff(attempt)
                            continue
                        return ino_err(
                            resp.reason or f"HTTP {status}",
                            status_code=status,
                            headers={k: v for k, v in resp.headers.items()},
                            path="",
                            bytes="",
                            filename="",
                            url=full_url,
                            method="GET",
                            attempts=attempt,
                        )

                    # Retry on configured statuses
                    if status in self._retry_for_statuses and attempt < attempts:
                        await self._sleep_backoff(attempt)
                        continue

                    # Handle resume/non-resume semantics
                    if start_offset > 0 and status == 200:
                        # Server ignored range; restart from scratch
                        try:
                            tmp.unlink(missing_ok=True)
                        except Exception:
                            pass
                        start_offset = 0

                    # Determine expected total size
                    total_size: Optional[int] = None
                    content_length = resp.headers.get("Content-Length")
                    if content_length and content_length.isdigit():
                        total_size = int(content_length)
                        if status == 206:
                            # For partial content, Content-Length is remaining part; total is start + remaining
                            total_size = start_offset + int(content_length)
                    # Try Content-Range for 206
                    if status == 206 and total_size is None:
                        cr = resp.headers.get("Content-Range")
                        # Format: bytes start-end/total
                        if cr and "/" in cr:
                            try:
                                total_part = cr.split("/")[-1]
                                if total_part.isdigit():
                                    total_size = int(total_part)
                            except Exception:
                                total_size = None
                    if total_size is None and range_probe_total_size is not None:
                        total_size = range_probe_total_size

                    # If dest was a directory and no explicit filename, try to derive a better final name now
                    if is_dir_hint and not filename:
                        headers_out_case = {k: v for k, v in resp.headers.items()}
                        # Best-effort filename derivation
                        cd = headers_out_case.get("Content-Disposition") or headers_out_case.get("content-disposition") or ""
                        derived: Optional[str] = None
                        if cd:
                            m = re.search(r"filename\*=([^']*)''([^;]+)", cd)
                            if m:
                                derived = unquote(m.group(2).strip())
                            if not derived:
                                m = re.search(r'filename\s*=\s*"([^"]+)"', cd)
                                if m:
                                    derived = m.group(1).strip()
                            if not derived:
                                m = re.search(r"filename\s*=\s*([^;]+)", cd)
                                if m:
                                    derived = m.group(1).strip().strip("'\"")
                        if not derived:
                            path_name = Path(unquote(urlparse(str(resp.url)).path)).name
                            if path_name and not path_name.endswith("/") and "." in path_name and not path_name.startswith("."):
                                derived = path_name
                        # Extension from content-type if missing
                        ext = Path(derived).suffix if derived else ""
                        if not ext:
                            ctype = (headers_out_case.get("Content-Type") or headers_out_case.get("content-type") or "").split(";")[0].strip().lower()
                            if ctype:
                                guessed = mimetypes.guess_extension(ctype, strict=False)
                                if guessed:
                                    ext = guessed
                        if not derived:
                            derived = "download" + (ext or "")
                        derived = _sanitize_filename(derived)
                        if derived and derived != dest.name:
                            candidate = base_dir / derived
                            if candidate.exists() and not overwrite:
                                return ino_err(
                                    f"Destination exists and overwrite=False: {candidate}",
                                    status_code=None,
                                    headers={},
                                    path="",
                                    bytes="",
                                    filename="",
                                    url=full_url,
                                    method="GET",
                                    attempts=attempt,
                                )
                            # do not change tmp to preserve resume continuity; only change final dest
                            dest = candidate

                    mode = "ab" if start_offset > 0 else "wb"
                    bytes_downloaded = start_offset

                    if progress:
                        try:
                            progress(bytes_downloaded, total_size)
                        except Exception:
                            # Don't fail download because of progress callback
                            pass

                    # Multi-connection download is only used when requested and supported.
                    if (
                        requested_connections > 1
                        and range_supported
                        and start_offset == 0
                        and total_size is not None
                        and total_size > 0
                    ):
                        # Abort this probe/download response before starting ranged workers.
                        # `release()` may drain unread body bytes, which can keep network traffic
                        # going unnecessarily for large files.
                        resp.close()
                        try:
                            bytes_downloaded = await self._download_multi_connection(
                                full_url,
                                tmp=tmp,
                                total_size=total_size,
                                connections=requested_connections,
                                chunk_size=chunk_size,
                                params=params,
                                headers=merged_headers,
                                timeout=timeout,
                                allow_redirects=allow_redirects,
                                auth=auth_obj,
                                progress=progress,
                            )
                        except Exception:
                            # If concurrent range mode fails unexpectedly, fall back to single stream.
                            try:
                                tmp.unlink(missing_ok=True)
                            except Exception:
                                pass
                            range_supported = False
                            attempt -= 1
                            continue
                    else:
                        with open(tmp, mode) as f:
                            async for chunk in resp.content.iter_chunked(max(1, int(chunk_size))):
                                if not chunk:
                                    continue
                                f.write(chunk)
                                bytes_downloaded += len(chunk)
                                if progress:
                                    try:
                                        progress(bytes_downloaded, total_size)
                                    except Exception:
                                        pass

                    # Verify size if requested and known
                    if verify_size and total_size is not None and bytes_downloaded != total_size:
                        # Size mismatch: consider retry if attempts left
                        raise IOError(
                            f"Downloaded size mismatch: got {bytes_downloaded}, expected {total_size}"
                        )

                    # Finalize: replace destination atomically
                    try:
                        if dest.exists():
                            os.replace(tmp, dest)
                        else:
                            tmp.replace(dest)
                    except FileNotFoundError:
                        # Parent may have been deleted concurrently; recreate and retry replace
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(tmp, dest)

                    headers_out = {k: v for k, v in resp.headers.items()}
                    return ino_ok(
                        resp.reason or "",
                        status_code=status,
                        headers=headers_out,
                        path=str(dest),
                        bytes=bytes_downloaded,
                        filename=Path(dest).name,
                        url=full_url,
                        method="GET",
                        attempts=attempt,
                    )

            except aiohttp.ClientResponseError as cre:
                last_exc = cre
                if getattr(cre, "status", None) in self._retry_for_statuses and attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                return ino_err(
                    str(cre),
                    status_code=getattr(cre, "status", None),
                    headers={},
                    path="",
                    bytes="",
                    filename="",
                    url=full_url,
                    method="GET",
                    attempts=attempt,
                )
            except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError, aiohttp.ClientOSError, aiohttp.TooManyRedirects) as ce:
                last_exc = ce
                if attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                return ino_err(
                    str(ce),
                    status_code=None,
                    headers={},
                    path="",
                    bytes="",
                    filename="",
                    url=full_url,
                    method="GET",
                    attempts=attempt,
                )
            except asyncio.TimeoutError as te:
                last_exc = te
                if attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                return ino_err(
                    "Request timed out: " + str(te),
                    status_code=None,
                    headers={},
                    path="",
                    bytes="",
                    filename="",
                    url=full_url,
                    method="GET",
                    attempts=attempt,
                )
            except Exception as e:
                # For IO errors or size mismatch etc., retry if possible
                last_exc = e
                if attempt < attempts:
                    await self._sleep_backoff(attempt)
                    continue
                return ino_err(
                    str(e),
                    status_code=None,
                    headers={},
                    path="",
                    bytes="",
                    filename="",
                    url=full_url,
                    method="GET",
                    attempts=attempt,
                )

        # If all attempts failed
        return ino_err(
            str(last_exc) if last_exc else "Download failed",
            status_code=getattr(last_exc, "status", None) if last_exc else None,
            headers={},
            path="",
            bytes="",
            filename="",
            url=full_url,
            method="GET",
            attempts=attempts,
        )

    async def _probe_range_support(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        allow_redirects: bool = True,
        auth: Optional[aiohttp.BasicAuth] = None,
    ) -> Tuple[bool, Optional[int]]:
        probe_headers = dict(headers or {})
        probe_headers["Range"] = "bytes=0-0"
        try:
            async with self._session.get(
                url,
                params=params,
                headers=probe_headers,
                timeout=timeout,
                allow_redirects=allow_redirects,
                auth=auth,
            ) as resp:
                if resp.status == 206:
                    total = _parse_total_from_content_range(resp.headers.get("Content-Range", ""))
                    if total is None:
                        content_length = resp.headers.get("Content-Length")
                        if content_length and content_length.isdigit():
                            total = int(content_length)
                    return True, total

                content_length = resp.headers.get("Content-Length")
                total = int(content_length) if content_length and content_length.isdigit() else None
                return False, total
        except Exception:
            return False, None

    async def _download_multi_connection(
        self,
        url: str,
        *,
        tmp: Path,
        total_size: int,
        connections: int,
        chunk_size: int,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        allow_redirects: bool = True,
        auth: Optional[aiohttp.BasicAuth] = None,
        progress: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> int:
        part_count = max(1, min(connections, total_size))
        base_headers = dict(headers or {})

        with open(tmp, "wb") as f:
            f.truncate(total_size)

        ranges = []
        part_size = total_size // part_count
        cursor = 0
        for i in range(part_count):
            start = cursor
            if i == part_count - 1:
                end = total_size - 1
            else:
                end = start + part_size - 1
            ranges.append((start, end))
            cursor = end + 1

        downloaded = 0
        progress_lock = asyncio.Lock()

        async def _worker(start: int, end: int) -> None:
            nonlocal downloaded
            req_headers = dict(base_headers)
            req_headers["Range"] = f"bytes={start}-{end}"
            async with self._session.get(
                url,
                params=params,
                headers=req_headers,
                timeout=timeout,
                allow_redirects=allow_redirects,
                auth=auth,
            ) as part_resp:
                if part_resp.status != 206:
                    raise IOError(f"Range request failed with status {part_resp.status}")

                remaining = end - start + 1
                with open(tmp, "r+b") as f:
                    f.seek(start)
                    async for chunk in part_resp.content.iter_chunked(max(1, int(chunk_size))):
                        if not chunk:
                            continue
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]
                        f.write(chunk)
                        got = len(chunk)
                        remaining -= got
                        async with progress_lock:
                            downloaded += got
                            if progress:
                                try:
                                    progress(downloaded, total_size)
                                except Exception:
                                    pass
                        if remaining <= 0:
                            break

                if remaining != 0:
                    raise IOError("Range download ended before expected bytes were received")

        await asyncio.gather(*(_worker(start, end) for start, end in ranges))
        return downloaded

    async def _sleep_backoff(self, attempt: int) -> None:
        delay = self._backoff_factor * (2 ** (attempt - 1))
        # Add a small jitter to avoid thundering herd
        delay *= 1 + 0.1 * (attempt % 3)
        await asyncio.sleep(delay)
