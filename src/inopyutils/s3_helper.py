import asyncio
import aiofiles
from aioboto3 import Session
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError, ConnectionClosedError
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from pathlib import Path
from typing import Optional, Dict, Any, Callable, Awaitable
import logging
import random
import mimetypes
from urllib.parse import quote


class InoS3Helper:
    """
    Async S3 client class that wraps aioboto3 functionality
    
    Compatible with AWS S3 and S3-compatible storage services including:
    - Amazon S3
    - Backblaze B2
    - DigitalOcean Spaces
    - Wasabi
    - MinIO
    - And other S3-compatible services
    
    Example usage with Backblaze B2:
        s3_client = InoS3Helper(
            aws_access_key_id="your_b2_key_id",
            aws_secret_access_key="your_b2_application_key",
            endpoint_url="https://s3.us-west-000.backblazeb2.com",
            region_name="us-west-000",
            bucket_name="your-bucket-name"
        )
    """

    def __init__(self, aws_access_key_id: Optional[str] = None, aws_secret_access_key: Optional[str] = None, aws_session_token: Optional[str] = None, region_name: str = "us-east-1", bucket_name: Optional[str] = None, endpoint_url: Optional[str] = None, retries: int = 3, config: Optional[Config] = None):
        self.region_name = region_name
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.retries = retries
        self.config: Optional[Config] = None
        self.session = None
        self.use_emoji: bool = True
        self.transfer_config: Optional[TransferConfig] = None
        # Always call init to set up session, config, and transfer_config
        self.init(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
            bucket_name=bucket_name,
            endpoint_url=endpoint_url,
            retries=retries,
            config=config,
        )

    def init(
            self,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            aws_session_token: Optional[str] = None,
            region_name: str = "us-east-1",
            bucket_name: Optional[str] = None,
            endpoint_url: Optional[str] = None,
            retries: int = 3,
            config: Optional[Config] = None
    ):
        """
        Initialize S3 client with AWS credentials and configuration

        Compatible with AWS S3 and S3-compatible storage services like Backblaze B2.

        Args:
            aws_access_key_id: AWS access key ID (optional if using env vars or IAM)
            aws_secret_access_key: AWS secret access key (optional if using env vars or IAM)
            aws_session_token: AWS session token (optional, for temporary credentials)
            region_name: AWS region name (default: us-east-1)
            bucket_name: Default bucket name for operations (optional)
            endpoint_url: Custom endpoint URL for S3-compatible services (e.g., Backblaze B2)
            retries: Number of retry attempts for failed operations (default: 3)
            config: Optional botocore.config.Config for fine-tuning (timeouts, retries, signature version, etc.)
        """

        self.region_name = region_name
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.retries = retries

        # Build default botocore Config with sane timeouts and addressing style
        default_cfg = Config(
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=10,
            read_timeout=60,
            signature_version="s3v4",
            s3={"addressing_style": "path" if endpoint_url else "auto"},
            region_name=region_name,
        )
        # Merge/choose provided config
        self.config = config or default_cfg

        # Default transfer configuration tuned for async (no threads)
        self.transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            max_concurrency=10,
            multipart_chunksize=8 * 1024 * 1024,
            use_threads=False,
        )

        if aws_access_key_id and aws_secret_access_key:
            self.session = Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name
            )
        else:
            self.session = Session(region_name=region_name)

    async def close(self) -> None:
        """Clean up resources. Safe to call multiple times."""
        self.session = None

    async def __aenter__(self) -> "InoS3Helper":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def _require_session(self) -> Session:
        """Return the aioboto3 Session, raising if not initialized."""
        if self.session is None:
            raise RuntimeError(
                "InoS3Helper is not initialized. Pass credentials to the constructor or call init() first."
            )
        return self.session

    def _validate_bucket(self, bucket_name: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Validate bucket name and return error dict if invalid, None if valid
        
        Args:
            bucket_name: Bucket name to validate
            
        Returns:
            None if valid, error dict if invalid
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket"
            }
        return None

    def _normalize_key(self, key: Optional[str]) -> Optional[str]:
        """Normalize S3 key: backslashes to slashes, strip leading slash, collapse doubles.
        Accepts Optional[str] and returns Optional[str] for convenience; returns None unchanged.
        """
        if key is None:
            return None
        k = key.replace("\\", "/").lstrip("/")
        while "//" in k:
            k = k.replace("//", "/")
        return k

    async def _retry_operation(
            self,
            operation: Callable[[], Awaitable[Dict[str, Any]]],
            operation_name: str
    ) -> Dict[str, Any]:
        """
        Retry an operation with exponential backoff
        
        Args:
            operation: Async function to retry
            operation_name: Name of the operation for logging
            
        Returns:
            Dict with "success", "msg", and optional "error_code"
        """
        last_exception = None
        
        for attempt in range(self.retries + 1):  # +1 for initial attempt
            try:
                result = await operation()
                if result.get("success", False):
                    return result
                # Honor explicit retryable flag from the operation result
                if result.get("retryable"):
                    if attempt < self.retries:
                        wait_time = (2 ** attempt) + random.uniform(0, 1)
                        logging.warning(f"{operation_name} attempt {attempt + 1} returned retryable failure, retrying in {wait_time:.2f}s: {result}")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        # Out of attempts; return last result
                        return result
                # If operation returns unsuccessful result without retryable flag, don't retry
                return result
            except (FileNotFoundError, NoCredentialsError, ValueError) as e:
                error_msg = f"❌ {operation_name} failed with non-retryable error: {str(e)}"
                logging.error(error_msg)
                return {
                    "success": False,
                    "msg": error_msg,
                    "error_code": type(e).__name__
                }
            except ClientError as e:
                err = e.response.get("Error", {}) if hasattr(e, "response") else {}
                error_code = err.get("Code", "")
                status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) if hasattr(e, "response") else 0
                non_retryable = {"NoSuchBucket", "NoSuchKey", "AccessDenied", "InvalidAccessKeyId"}
                retryable_codes = {"SlowDown", "Throttling", "RequestTimeout", "InternalError", "RequestTimeTooSkewed"}
                # Determine if retryable: 5xx or known retryable 4xx
                is_retryable = (error_code in retryable_codes) or (isinstance(status, int) and status >= 500)
                if (error_code in non_retryable) or (isinstance(status, int) and 400 <= status < 500 and not is_retryable):
                    error_msg = f"❌ {operation_name} failed with non-retryable client error {error_code}: {str(e)}"
                    logging.error(error_msg)
                    return {
                        "success": False,
                        "msg": error_msg,
                        "error_code": error_code or "ClientError"
                    }
                last_exception = e
                if attempt < self.retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"{operation_name} attempt {attempt + 1} failed with {error_code or status}, retrying in {wait_time:.2f}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    error_msg = f"❌ {operation_name} failed after {self.retries + 1} attempts with client error {error_code or status}: {str(e)}"
                    logging.error(error_msg)
                    return {
                        "success": False,
                        "msg": error_msg,
                        "error_code": error_code or "ClientError"
                    }
            except (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError, ConnectionClosedError) as e:
                last_exception = e
                if attempt < self.retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"{operation_name} attempt {attempt + 1} failed with transient network error {type(e).__name__}, retrying in {wait_time:.2f}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    error_msg = f"❌ {operation_name} failed after {self.retries + 1} attempts due to network error {type(e).__name__}: {str(e)}"
                    logging.error(error_msg)
                    return {
                        "success": False,
                        "msg": error_msg,
                        "error_code": type(e).__name__
                    }
            except Exception as e:
                last_exception = e
                if attempt < self.retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"{operation_name} attempt {attempt + 1} failed, retrying in {wait_time:.2f}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    error_msg = f"❌ {operation_name} failed after {self.retries + 1} attempts: {str(e)}"
                    logging.error(error_msg)
                    return {
                        "success": False,
                        "msg": error_msg,
                        "error_code": type(e).__name__
                    }
        
        # This should never be reached, but just in case
        return {
            "success": False,
            "msg": f"❌ {operation_name} failed unexpectedly",
            "error_code": "UnknownError"
        }

    async def upload_file(
            self,
            local_file_path: str,
            s3_key: str,
            bucket_name: Optional[str] = None,
            extra_args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Upload a file to S3 with automatic retry on failure

        Args:
            local_file_path: Path to the local file to upload
            s3_key: S3 key (path) where the file will be stored
            bucket_name: S3 bucket name (uses default if not provided)
            extra_args: Extra arguments for the upload (e.g., metadata, ACL)

        Returns:
            Dict with "success", "msg", "s3_key", "bucket", and optional "error_code"
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key)

        # Check if local file exists
        if not Path(local_file_path).exists():
            return {
                "success": False,
                "msg": f"❌ Local file not found: {local_file_path}",
                "error_code": "FileNotFound"
            }

        async def _upload_operation() -> Dict[str, Any]:
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                local_extra_args = dict(extra_args or {})
                if "ContentType" not in local_extra_args:
                    guess, _ = mimetypes.guess_type(local_file_path)
                    if guess:
                        local_extra_args["ContentType"] = guess
                await s3.upload_file(
                    local_file_path,
                    bucket,
                    norm_key,
                    ExtraArgs=local_extra_args,
                    Config=self.transfer_config
                )
                success_msg = f"✅ Successfully uploaded {Path(local_file_path).name} to s3://{bucket}/{norm_key}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": norm_key,
                    "bucket": bucket,
                    "local_file": local_file_path
                }

        return await self._retry_operation(
            _upload_operation,
            f"upload_file({local_file_path} -> s3://{bucket}/{norm_key})"
        )

    async def upload_file_object(
            self,
            local_file_path: str,
            s3_key: str,
            bucket_name: Optional[str] = None,
            content_type: str = "application/octet-stream",
            metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Upload a file with ExtraArgs control over metadata/content type, with automatic retry on failure

        Args:
            local_file_path: Path to the local file to upload
            s3_key: S3 key (path) where the file will be stored
            bucket_name: S3 bucket name (uses default if not provided)
            content_type: MIME type of the file
            metadata: Custom metadata to attach to the object

        Returns:
            Dict with "success", "msg", "s3_key", "bucket", "content_type", and optional "error_code"
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key)

        # Check if local file exists
        if not Path(local_file_path).exists():
            return {
                "success": False,
                "msg": f"❌ Local file not found: {local_file_path}",
                "error_code": "FileNotFound"
            }

        async def _upload_operation() -> Dict[str, Any]:
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                # Build ExtraArgs with sensible defaults and optional overrides
                guess, _ = mimetypes.guess_type(local_file_path)
                effective_content_type = content_type
                if (not content_type or content_type == "application/octet-stream") and guess:
                    effective_content_type = guess
                extra_args: Dict[str, Any] = {"ContentType": effective_content_type}
                if metadata:
                    extra_args["Metadata"] = metadata
                await s3.upload_file(
                    local_file_path,
                    bucket,
                    norm_key,
                    ExtraArgs=extra_args,
                    Config=self.transfer_config
                )

                success_msg = f"✅ Successfully uploaded {Path(local_file_path).name} to s3://{bucket}/{norm_key} with content type {effective_content_type}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": norm_key,
                    "bucket": bucket,
                    "content_type": effective_content_type,
                    "local_file": local_file_path,
                    "metadata": metadata or {}
                }

        return await self._retry_operation(
            _upload_operation,
            f"upload_file_object({local_file_path} -> s3://{bucket}/{norm_key})"
        )

    async def download_file(
            self,
            s3_key: str,
            local_file_path: str,
            bucket_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Download a file from S3 with automatic retry on failure

        Args:
            s3_key: S3 key (path) of the file to download
            local_file_path: Local path where the file will be saved
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            Dict with "success", "msg", "s3_key", "bucket", "local_file", and optional "error_code"
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key)

        async def _download_operation() -> Dict[str, Any]:
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                await s3.download_file(bucket, norm_key, local_file_path)
                success_msg = f"✅ Successfully downloaded s3://{bucket}/{norm_key} to {local_file_path}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": norm_key,
                    "bucket": bucket,
                    "local_file": local_file_path
                }

        return await self._retry_operation(
            _download_operation,
            f"download_file(s3://{bucket}/{norm_key} -> {local_file_path})"
        )

    async def download_file_object(
            self,
            s3_key: str,
            local_file_path: str,
            bucket_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Download a file using get_object for more control with automatic retry on failure

        Args:
            s3_key: S3 key (path) of the file to download
            local_file_path: Local path where the file will be saved
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            Dict with "success", "msg", "s3_key", "bucket", "local_file", and optional "error_code"
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key)

        async def _download_operation() -> Dict[str, Any]:
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                response = await s3.get_object(Bucket=bucket, Key=norm_key)
                stream = response["Body"]
                async with aiofiles.open(local_file_path, "wb") as file:
                    async for chunk in stream.iter_chunks():
                        if chunk:
                            await file.write(chunk)
                # Explicit close if available
                try:
                    await stream.close()
                except Exception:
                    pass

                success_msg = f"✅ Successfully downloaded s3://{bucket}/{norm_key} to {local_file_path}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": norm_key,
                    "bucket": bucket,
                    "local_file": local_file_path
                }

        return await self._retry_operation(
            _download_operation,
            f"download_file_object(s3://{bucket}/{norm_key} -> {local_file_path})"
        )

    async def list_objects(
            self,
            prefix: str = "",
            bucket_name: Optional[str] = None,
            max_keys: int = 1000,
            recursive: bool = True,
            delimiter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List objects in S3 bucket

        Args:
            prefix: Filter objects by prefix
            bucket_name: S3 bucket name (uses default if not provided)
            max_keys: Maximum number of objects to return

        Returns:
            Dict with "success", "msg", "objects", "count", and optional "common_prefixes" and "error_code"
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err | {"objects": [], "count": 0}
        bucket = bucket_name or self.bucket_name
        norm_prefix = self._normalize_key(prefix) or ""

        # Input validation
        if max_keys <= 0:
            return {
                "success": False,
                "msg": "❌ max_keys must be greater than 0",
                "error_code": "InvalidParameter",
                "objects": [],
                "count": 0
            }

        async def _list_operation() -> Dict[str, Any]:
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                all_objects = []
                common_prefixes_accum = []
                token = None
                while True:
                    params: Dict[str, Any] = {"Bucket": bucket, "Prefix": norm_prefix, "MaxKeys": 1000}
                    if not recursive:
                        params["Delimiter"] = delimiter or "/"
                    if token:
                        params["ContinuationToken"] = token
                    resp = await s3.list_objects_v2(**params)
                    # collect prefixes if non-recursive
                    if not recursive:
                        cps = resp.get("CommonPrefixes") or []
                        for cp in cps:
                            p = cp.get("Prefix")
                            if p is not None:
                                common_prefixes_accum.append(p)
                    for obj in resp.get("Contents", []) or []:
                        # skip folder markers
                        if obj.get("Key", "").endswith("/"):
                            continue
                        all_objects.append({
                            "Key": obj["Key"],
                            "Size": obj["Size"],
                            "LastModified": obj["LastModified"],
                            "ETag": obj["ETag"],
                        })
                    if len(all_objects) >= max_keys:
                        break
                    if not resp.get("IsTruncated"):
                        break
                    token = resp.get("NextContinuationToken")
                out = all_objects[:max_keys]
                success_msg = f"✅ Found {len(out)} objects in s3://{bucket} with prefix {norm_prefix}"
                logging.info(success_msg)
                result: Dict[str, Any] = {
                    "success": True,
                    "msg": success_msg,
                    "objects": out,
                    "count": len(out),
                    "bucket": bucket,
                    "prefix": norm_prefix
                }
                if not recursive:
                    result["common_prefixes"] = common_prefixes_accum
                return result

        return await self._retry_operation(
            _list_operation,
            f"list_objects(s3://{bucket}, prefix={norm_prefix})"
        )

    async def count_files_in_folder(
            self,
            s3_folder_key: str,
            bucket_name: Optional[str] = None,
            recursive: bool = True,
    ) -> Dict[str, Any]:
        """
        Count files under an S3 "folder" (prefix).

        Args:
            s3_folder_key: The folder key (prefix). Can be provided with or without a trailing "/".
            bucket_name: Optional bucket override. Uses initialized default if not provided.
            recursive: If True, count all files under the prefix recursively. If False, count only
                       immediate files directly under the folder (no subfolders).

        Returns:
            Dict with fields: success, msg, count, bucket, s3_folder_key, recursive, and error_code on failure.
        """
        err = self._validate_bucket(bucket_name)
        if err:
            # Augment with count for convenience
            return {**err, "count": 0, "recursive": recursive}

        bucket = bucket_name or self.bucket_name

        # Normalize and ensure trailing slash for folder semantics
        prefix = self._normalize_key(s3_folder_key)
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        async def _count_op() -> Dict[str, Any]:
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                total = 0
                token: Optional[str] = None
                while True:
                    params: Dict[str, Any] = {
                        "Bucket": bucket,
                        "Prefix": prefix,
                        "MaxKeys": 1000,
                    }
                    # For non-recursive listing, use delimiter to collapse common prefixes
                    if not recursive:
                        params["Delimiter"] = "/"
                    if token:
                        params["ContinuationToken"] = token

                    resp = await s3.list_objects_v2(**params)
                    contents = resp.get("Contents", []) or []
                    # Count only actual objects (skip folder markers)
                    for obj in contents:
                        key = obj.get("Key", "")
                        if not key.endswith("/"):
                            total += 1

                    if not resp.get("IsTruncated", False):
                        break
                    token = resp.get("NextContinuationToken")

                msg = f"✅ Found {total} files in s3://{bucket}/{prefix} (recursive={recursive})"
                logging.info(msg)
                return {
                    "success": True,
                    "msg": msg,
                    "count": total,
                    "bucket": bucket,
                    "s3_folder_key": prefix,
                    "recursive": recursive,
                }

        return await self._retry_operation(
            _count_op,
            f"count_files_in_folder(s3://{bucket}/{prefix}, recursive={recursive})"
        )

    async def delete_object(
            self,
            s3_key: str,
            bucket_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete an object from S3

        Args:
            s3_key: S3 key (path) of the file to delete
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            Dict with "success", "msg", "s3_key", "bucket", and optional "error_code"
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key)

        async def _delete_operation() -> Dict[str, Any]:
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                await s3.delete_object(Bucket=bucket, Key=norm_key)
                success_msg = f"✅ Successfully deleted s3://{bucket}/{norm_key}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": norm_key,
                    "bucket": bucket
                }

        return await self._retry_operation(
            _delete_operation,
            f"delete_object(s3://{bucket}/{norm_key})"
        )

    async def download_folder(
            self,
            s3_folder_key: str,
            local_folder_path: str,
            bucket_name: Optional[str] = None,
            max_concurrent: int = 5,
            verify: bool = False
    ) -> Dict[str, Any]:
        """
        Download an entire folder from S3, preserving directory structure locally

        Args:
            s3_folder_key: S3 key (path) of the folder to download (should end with "/")
            local_folder_path: Local directory path where the folder will be saved
            bucket_name: S3 bucket name (uses default if not provided)
            max_concurrent: Maximum number of concurrent downloads (default: 5)

        Returns:
            Dict[str, Any]: Status information with success/failure counts and details
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return {**err, "total_files": 0, "downloaded_successfully": 0, "failed_downloads": 0, "errors": []}
        bucket = bucket_name or self.bucket_name
        s3_folder_key = self._normalize_key(s3_folder_key)
        if not s3_folder_key.endswith("/"):
            s3_folder_key += "/"

        local_folder = Path(local_folder_path)
        local_folder.mkdir(parents=True, exist_ok=True)

        result: Dict[str, Any] = {
            "success": True,
            "msg": "",
            "total_files": 0,
            "downloaded_successfully": 0,
            "failed_downloads": 0,
            "errors": [],
            "bucket": bucket,
            "s3_folder_key": s3_folder_key,
            "local_folder_path": local_folder_path
        }

        try:
            all_objects = []
            continuation_token = None
            
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                while True:
                    list_params = {
                        "Bucket": bucket,
                        "Prefix": s3_folder_key,
                        "MaxKeys": 1000
                    }
                    
                    if continuation_token:
                        list_params["ContinuationToken"] = continuation_token

                    response = await s3.list_objects_v2(**list_params)

                    if "Contents" in response:
                        all_objects.extend(response["Contents"])

                    if not response.get("IsTruncated", False):
                        break
                    
                    continuation_token = response.get("NextContinuationToken")

                # Filter out directory markers (keys ending with "/")
                file_objects = [obj for obj in all_objects if not obj["Key"].endswith("/")]
                result["total_files"] = len(file_objects)
                
                logging.info(f"Found {result['total_files']} files to download from s3://{bucket}/{s3_folder_key}")

                # Download files concurrently with semaphore to limit concurrent operations
                semaphore = asyncio.Semaphore(max_concurrent)
                
                async def _download_single_file_with_semaphore(obj: Dict[str, Any]) -> Dict[str, Any]:
                    """Download a single file with semaphore control"""
                    async with semaphore:
                        s3_key = obj["Key"]
                        relative_path = s3_key[len(s3_folder_key):]
                        local_file_path = local_folder / relative_path
                        
                        try:
                            # Ensure parent directories exist for nested files
                            local_file_path.parent.mkdir(parents=True, exist_ok=True)
                            # Use the shared client to download
                            await s3.download_file(bucket, s3_key, str(local_file_path))
                            return {
                                "s3_key": s3_key,
                                "relative_path": relative_path,
                                "result": {
                                    "success": True,
                                    "msg": f"Downloaded {relative_path}",
                                }
                            }
                        except Exception as e:
                            return {
                                "s3_key": s3_key,
                                "relative_path": relative_path,
                                "result": {
                                    "success": False,
                                    "msg": f"Exception during download: {str(e)}",
                                    "error_code": type(e).__name__
                                }
                            }

                # Execute all downloads concurrently
                download_tasks = [_download_single_file_with_semaphore(obj) for obj in file_objects]
                download_results = await asyncio.gather(*download_tasks, return_exceptions=True)

            # Process results
            for download_result in download_results:
                if isinstance(download_result, Exception):
                    result["failed_downloads"] += 1
                    error_msg = f"Download task failed with exception: {str(download_result)}"
                    result["errors"].append(error_msg)
                    logging.error(error_msg)
                    continue

                s3_key = download_result["s3_key"]
                relative_path = download_result["relative_path"]
                download_outcome = download_result["result"]
                
                if download_outcome.get("success", False):
                    result["downloaded_successfully"] += 1
                    logging.debug(f"Successfully downloaded {relative_path}")
                else:
                    result["failed_downloads"] += 1
                    error_msg = f"Failed to download {relative_path}: {download_outcome.get('msg', 'Unknown error')}"
                    result["errors"].append(error_msg)
                    logging.error(error_msg)

            result["success"] = result["failed_downloads"] == 0
            
            if result["success"]:
                result["msg"] = f"✅ Successfully downloaded folder s3://{bucket}/{s3_folder_key} to {local_folder_path} ({result['downloaded_successfully']} files)"
                logging.info(result["msg"])
            else:
                result["msg"] = f"❌ Folder download completed with {result['failed_downloads']} failures. Downloaded {result['downloaded_successfully']}/{result['total_files']} files"
                result["error_code"] = "PartialFailure"
                logging.warning(result["msg"])

        except Exception as e:
            error_msg = f"❌ Error downloading folder s3://{bucket}/{s3_folder_key}: {str(e)}"
            logging.error(error_msg)
            result["success"] = False
            result["msg"] = error_msg
            result["error_code"] = type(e).__name__
            result["errors"].append(error_msg)

        # Optional post-download verification
        if verify:
            try:
                verification = await self.verify_folder_sync(
                    s3_folder_key=s3_folder_key,
                    local_folder_path=local_folder_path,
                    bucket_name=bucket
                )
                result["verification"] = verification
                if not verification.get("success", False):
                    result["success"] = False
                    result["error_code"] = "VerificationFailed"
                    result["msg"] = f"❌ Downloaded with verification mismatches: {verification.get('summary', verification.get('msg', 'Mismatch found'))}"
                    logging.error(result["msg"])
            except Exception as ve:
                # Do not fail the main operation if verification step errors; report it
                ver_msg = f"⚠️ Verification step failed: {str(ve)}"
                logging.warning(ver_msg)
                result["verification"] = {
                    "success": False,
                    "msg": ver_msg,
                    "error_code": type(ve).__name__
                }

        return result

    async def sync_folder(
            self,
            s3_key: str,
            local_folder_path: str,
            sync_local: bool = True,
            concc: int = 5,
            bucket_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Robustly synchronize files between an S3 prefix and a local folder.

        Behavior when sync_local=True (S3 -> local):
        - List all remote files under the provided prefix
        - Download missing/changed local files
        - Verify downloaded/kept files using hash when possible (SHA256/MD5 via verify_file)
        - Remove local files that are not present remotely under that prefix

        Behavior when sync_local=False (local -> S3):
        - Scan all local files under the provided folder
        - Upload missing/changed files to S3
        - Verify uploaded files using hash when possible (SHA256/MD5 via verify_file)
        - Remove remote objects that are not present locally

        Args:
            s3_key: S3 key prefix to sync from/to
            local_folder_path: Local folder path
            sync_local: If True, sync S3 to local. If False, sync local to S3
            concc: Maximum concurrent file sync operations
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            Dict[str, Any]: Sync status and detailed counters
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return {
                **err,
                "total_remote_files": 0,
                "total_local_files": 0,
                "downloaded": 0,
                "uploaded": 0,
                "updated": 0,
                "skipped_unchanged": 0,
                "verified": 0,
                "removed_local": 0,
                "removed_remote": 0,
                "failed": 0,
                "errors": []
            }

        bucket = bucket_name or self.bucket_name
        prefix = self._normalize_key(s3_key) or ""
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        local_folder = Path(local_folder_path)
        local_folder.mkdir(parents=True, exist_ok=True)

        result: Dict[str, Any] = {
            "success": True,
            "msg": "",
            "bucket": bucket,
            "s3_key": prefix,
            "local_folder_path": local_folder_path,
            "sync_local": sync_local,
            "concc": concc,
            "total_remote_files": 0,
            "total_local_files": 0,
            "downloaded": 0,
            "uploaded": 0,
            "updated": 0,
            "skipped_unchanged": 0,
            "verified": 0,
            "removed_local": 0,
            "removed_remote": 0,
            "failed": 0,
            "errors": []
        }

        if sync_local:
            return await self._sync_remote_to_local(
                bucket, prefix, local_folder, result, concc
            )
        else:
            return await self._sync_local_to_remote(
                bucket, prefix, local_folder, result, concc
            )

    async def _sync_remote_to_local(
            self,
            bucket: str,
            prefix: str,
            local_folder: Path,
            result: Dict[str, Any],
            concc: int
    ) -> Dict[str, Any]:
        """Internal: sync S3 prefix -> local folder (used when sync_local=True)."""
        max_concurrent = max(1, int(concc))
        file_attempt_limit = max(3, self.retries + 1)
        transfer_config = TransferConfig(
            multipart_threshold=64 * 1024 * 1024,
            multipart_chunksize=64 * 1024 * 1024,
            max_concurrency=max(2, min(16, max_concurrent)),
            num_download_attempts=max(10, self.retries + 1),
            use_threads=True,
        )

        try:
            remote_objects: Dict[str, Dict[str, Any]] = {}
            continuation_token = None

            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                while True:
                    params: Dict[str, Any] = {
                        "Bucket": bucket,
                        "Prefix": prefix,
                        "MaxKeys": 1000
                    }
                    if continuation_token:
                        params["ContinuationToken"] = continuation_token

                    response = await s3.list_objects_v2(**params)
                    for obj in response.get("Contents", []):
                        key = obj.get("Key", "")
                        if not key or key.endswith("/"):
                            continue
                        rel_key = key[len(prefix):] if prefix else key
                        rel_key = rel_key.replace("\\", "/")
                        remote_objects[rel_key] = obj

                    if not response.get("IsTruncated", False):
                        break
                    continuation_token = response.get("NextContinuationToken")

                result["total_remote_files"] = len(remote_objects)

                # Build local map under the sync root
                local_files: Dict[str, Path] = {}
                for file_path in local_folder.rglob("*"):
                    if file_path.is_file():
                        rel = file_path.relative_to(local_folder)
                        rel_norm = str(rel).replace("\\", "/")
                        local_files[rel_norm] = file_path

                remote_rel_set = set(remote_objects.keys())
                local_rel_set = set(local_files.keys())

                # Remove files not present in remote
                to_remove = sorted(local_rel_set - remote_rel_set)
                for rel in to_remove:
                    try:
                        local_files[rel].unlink(missing_ok=True)
                        result["removed_local"] += 1
                    except Exception as e:
                        result["failed"] += 1
                        msg = f"Failed to remove stale local file {local_files[rel]}: {str(e)}"
                        result["errors"].append(msg)
                        logging.error(msg)

                # Cleanup empty directories bottom-up
                dirs = sorted([p for p in local_folder.rglob("*") if p.is_dir()], key=lambda p: len(p.parts), reverse=True)
                for d in dirs:
                    try:
                        if not any(d.iterdir()):
                            d.rmdir()
                    except Exception:
                        pass

                semaphore = asyncio.Semaphore(max_concurrent)

                async def _sync_one(rel_key: str) -> Dict[str, Any]:
                    async with semaphore:
                        remote_obj = remote_objects[rel_key]
                        s3_obj_key = remote_obj.get("Key", "")
                        remote_size = int(remote_obj.get("Size", 0))
                        etag_raw = remote_obj.get("ETag")
                        etag = etag_raw.strip('"') if isinstance(etag_raw, str) else None

                        local_file = local_folder / rel_key
                        local_file.parent.mkdir(parents=True, exist_ok=True)
                        downloaded_any = False
                        updated_existing = False
                        last_verify_res: Dict[str, Any] = {
                            "success": False,
                            "msg": "Unknown verification state",
                            "error_code": "UnknownVerificationState"
                        }
                        last_error: Optional[Exception] = None

                        for attempt in range(file_attempt_limit):
                            must_download = True
                            if local_file.exists() and local_file.is_file():
                                local_size = local_file.stat().st_size
                                if local_size == remote_size:
                                    if etag and "-" not in etag:
                                        import hashlib
                                        h = hashlib.md5()
                                        async with aiofiles.open(local_file, "rb") as f:  # type: ignore
                                            while True:
                                                chunk = await f.read(1024 * 1024)
                                                if not chunk:
                                                    break
                                                h.update(chunk)
                                        if h.hexdigest() == etag:
                                            must_download = False
                                    else:
                                        must_download = False

                            try:
                                if must_download:
                                    if local_file.exists():
                                        local_file.unlink(missing_ok=True)
                                    await s3.download_file(bucket, s3_obj_key, str(local_file), Config=transfer_config)
                                    downloaded_any = True
                                    if rel_key in local_files:
                                        updated_existing = True

                                verify_res = await self.verify_file(
                                    local_file_path=str(local_file),
                                    s3_key=s3_obj_key,
                                    bucket_name=bucket,
                                    use_md5=True,
                                    use_sha256=True
                                )
                                last_verify_res = verify_res

                                if verify_res.get("success", False):
                                    return {
                                        "relative_path": rel_key,
                                        "downloaded": downloaded_any,
                                        "updated": updated_existing,
                                        "verify": verify_res,
                                        "attempts": attempt + 1,
                                    }

                                if local_file.exists():
                                    local_file.unlink(missing_ok=True)

                            except Exception as e:
                                last_error = e
                                if local_file.exists():
                                    local_file.unlink(missing_ok=True)

                            if attempt < file_attempt_limit - 1:
                                wait_time = min(30.0, (2 ** attempt) + random.uniform(0, 1))
                                await asyncio.sleep(wait_time)

                        if last_error is not None:
                            return {
                                "relative_path": rel_key,
                                "downloaded": downloaded_any,
                                "updated": updated_existing,
                                "verify": {
                                    "success": False,
                                    "msg": f"Failed after {file_attempt_limit} attempts: {str(last_error)}",
                                    "error_code": type(last_error).__name__
                                },
                                "attempts": file_attempt_limit,
                            }

                        return {
                            "relative_path": rel_key,
                            "downloaded": downloaded_any,
                            "updated": updated_existing,
                            "verify": {
                                **last_verify_res,
                                "msg": f"Failed verification after {file_attempt_limit} attempts: {last_verify_res.get('msg', 'Unknown error')}"
                            },
                            "attempts": file_attempt_limit,
                        }

                tasks = [_sync_one(rel) for rel in sorted(remote_objects.keys())]
                sync_results = await asyncio.gather(*tasks, return_exceptions=True)

            local_folder_path = str(local_folder)
            for sync_res in sync_results:
                if isinstance(sync_res, Exception):
                    result["failed"] += 1
                    msg = f"Sync task failed with exception: {str(sync_res)}"
                    result["errors"].append(msg)
                    logging.error(msg)
                    continue

                if sync_res.get("downloaded"):
                    if sync_res.get("updated"):
                        result["updated"] += 1
                    else:
                        result["downloaded"] += 1
                else:
                    result["skipped_unchanged"] += 1

                verify_res = sync_res.get("verify", {})
                if verify_res.get("success", False):
                    result["verified"] += 1
                else:
                    result["failed"] += 1
                    msg = f"Verification failed for {sync_res.get('relative_path')}: {verify_res.get('msg', 'Unknown error')}"
                    result["errors"].append(msg)
                    logging.error(msg)

            result["success"] = result["failed"] == 0
            if result["success"]:
                result["msg"] = (
                    f"✅ Sync completed for s3://{bucket}/{prefix} -> {local_folder_path} "
                    f"(remote={result['total_remote_files']}, downloaded={result['downloaded']}, "
                    f"updated={result['updated']}, removed_local={result['removed_local']})"
                )
                logging.info(result["msg"])
            else:
                result["msg"] = (
                    f"❌ Sync completed with failures for s3://{bucket}/{prefix} -> {local_folder_path}. "
                    f"failed={result['failed']}, verified={result['verified']}/{result['total_remote_files']}"
                )
                result["error_code"] = "PartialFailure"
                logging.warning(result["msg"])

        except Exception as e:
            local_folder_path = str(local_folder)
            error_msg = f"❌ Error syncing folder s3://{bucket}/{prefix} -> {local_folder_path}: {str(e)}"
            logging.error(error_msg)
            result["success"] = False
            result["msg"] = error_msg
            result["error_code"] = type(e).__name__
            result["failed"] += 1
            result["errors"].append(error_msg)

        return result

    async def _sync_local_to_remote(
            self,
            bucket: str,
            prefix: str,
            local_folder: Path,
            result: Dict[str, Any],
            concc: int
    ) -> Dict[str, Any]:
        """Internal: sync local folder -> S3 prefix (used when sync_local=False)."""
        max_concurrent = max(1, int(concc))
        file_attempt_limit = max(3, self.retries + 1)

        try:
            # Build local file map
            local_files: Dict[str, Path] = {}
            for file_path in local_folder.rglob("*"):
                if file_path.is_file():
                    rel = file_path.relative_to(local_folder)
                    rel_norm = str(rel).replace("\\", "/")
                    local_files[rel_norm] = file_path

            result["total_local_files"] = len(local_files)

            # List all remote objects under prefix
            remote_objects: Dict[str, Dict[str, Any]] = {}
            continuation_token = None

            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                while True:
                    params: Dict[str, Any] = {
                        "Bucket": bucket,
                        "Prefix": prefix,
                        "MaxKeys": 1000
                    }
                    if continuation_token:
                        params["ContinuationToken"] = continuation_token

                    response = await s3.list_objects_v2(**params)
                    for obj in response.get("Contents", []):
                        key = obj.get("Key", "")
                        if not key or key.endswith("/"):
                            continue
                        rel_key = key[len(prefix):] if prefix else key
                        rel_key = rel_key.replace("\\", "/")
                        remote_objects[rel_key] = obj

                    if not response.get("IsTruncated", False):
                        break
                    continuation_token = response.get("NextContinuationToken")

                result["total_remote_files"] = len(remote_objects)

                remote_rel_set = set(remote_objects.keys())
                local_rel_set = set(local_files.keys())

                # Remove remote objects not present locally
                to_remove = sorted(remote_rel_set - local_rel_set)
                for rel in to_remove:
                    remote_key = remote_objects[rel].get("Key", "")
                    try:
                        await s3.delete_object(Bucket=bucket, Key=remote_key)
                        result["removed_remote"] += 1
                    except Exception as e:
                        result["failed"] += 1
                        msg = f"Failed to remove remote object s3://{bucket}/{remote_key}: {str(e)}"
                        result["errors"].append(msg)
                        logging.error(msg)

                semaphore = asyncio.Semaphore(max_concurrent)

                async def _upload_one(rel_key: str) -> Dict[str, Any]:
                    async with semaphore:
                        local_file = local_files[rel_key]
                        s3_obj_key = prefix + rel_key
                        local_size = local_file.stat().st_size
                        uploaded_any = False
                        updated_existing = rel_key in remote_objects
                        last_verify_res: Dict[str, Any] = {
                            "success": False,
                            "msg": "Unknown verification state",
                            "error_code": "UnknownVerificationState"
                        }
                        last_error: Optional[Exception] = None
                        force_upload = False

                        for attempt in range(file_attempt_limit):
                            must_upload = True

                            # Check if remote already matches (skip on first attempt only if not forced)
                            if not force_upload and rel_key in remote_objects:
                                remote_obj = remote_objects[rel_key]
                                remote_size = int(remote_obj.get("Size", 0))
                                etag_raw = remote_obj.get("ETag")
                                etag = etag_raw.strip('"') if isinstance(etag_raw, str) else None

                                if local_size == remote_size:
                                    if etag and "-" not in etag:
                                        import hashlib
                                        h = hashlib.md5()
                                        async with aiofiles.open(local_file, "rb") as f:  # type: ignore
                                            while True:
                                                chunk = await f.read(1024 * 1024)
                                                if not chunk:
                                                    break
                                                h.update(chunk)
                                        if h.hexdigest() == etag:
                                            must_upload = False
                                    else:
                                        must_upload = False

                            try:
                                if must_upload:
                                    extra_args: Dict[str, str] = {}
                                    guess, _ = mimetypes.guess_type(str(local_file))
                                    if guess:
                                        extra_args["ContentType"] = guess
                                    await s3.upload_file(
                                        str(local_file),
                                        bucket,
                                        s3_obj_key,
                                        ExtraArgs=extra_args if extra_args else None,
                                        Config=self.transfer_config
                                    )
                                    uploaded_any = True

                                verify_res = await self.verify_file(
                                    local_file_path=str(local_file),
                                    s3_key=s3_obj_key,
                                    bucket_name=bucket,
                                    use_md5=True,
                                    use_sha256=True
                                )
                                last_verify_res = verify_res

                                if verify_res.get("success", False):
                                    return {
                                        "relative_path": rel_key,
                                        "uploaded": uploaded_any,
                                        "updated": updated_existing and uploaded_any,
                                        "verify": verify_res,
                                        "attempts": attempt + 1,
                                    }

                                # Verification failed, force re-upload next attempt
                                force_upload = True
                            except Exception as e:
                                last_error = e

                            if attempt < file_attempt_limit - 1:
                                wait_time = min(30.0, (2 ** attempt) + random.uniform(0, 1))
                                await asyncio.sleep(wait_time)

                        if last_error is not None:
                            return {
                                "relative_path": rel_key,
                                "uploaded": uploaded_any,
                                "updated": updated_existing and uploaded_any,
                                "verify": {
                                    "success": False,
                                    "msg": f"Failed after {file_attempt_limit} attempts: {str(last_error)}",
                                    "error_code": type(last_error).__name__
                                },
                                "attempts": file_attempt_limit,
                            }

                        return {
                            "relative_path": rel_key,
                            "uploaded": uploaded_any,
                            "updated": updated_existing and uploaded_any,
                            "verify": {
                                **last_verify_res,
                                "msg": f"Failed verification after {file_attempt_limit} attempts: {last_verify_res.get('msg', 'Unknown error')}"
                            },
                            "attempts": file_attempt_limit,
                        }

                tasks = [_upload_one(rel) for rel in sorted(local_files.keys())]
                sync_results = await asyncio.gather(*tasks, return_exceptions=True)

            for sync_res in sync_results:
                if isinstance(sync_res, Exception):
                    result["failed"] += 1
                    msg = f"Sync task failed with exception: {str(sync_res)}"
                    result["errors"].append(msg)
                    logging.error(msg)
                    continue

                if sync_res.get("uploaded"):
                    if sync_res.get("updated"):
                        result["updated"] += 1
                    else:
                        result["uploaded"] += 1
                else:
                    result["skipped_unchanged"] += 1

                verify_res = sync_res.get("verify", {})
                if verify_res.get("success", False):
                    result["verified"] += 1
                else:
                    result["failed"] += 1
                    msg = f"Verification failed for {sync_res.get('relative_path')}: {verify_res.get('msg', 'Unknown error')}"
                    result["errors"].append(msg)
                    logging.error(msg)

            result["success"] = result["failed"] == 0
            local_folder_path = str(local_folder)
            if result["success"]:
                result["msg"] = (
                    f"✅ Sync completed for {local_folder_path} -> s3://{bucket}/{prefix} "
                    f"(local={result['total_local_files']}, uploaded={result['uploaded']}, "
                    f"updated={result['updated']}, removed_remote={result['removed_remote']})"
                )
                logging.info(result["msg"])
            else:
                result["msg"] = (
                    f"❌ Sync completed with failures for {local_folder_path} -> s3://{bucket}/{prefix}. "
                    f"failed={result['failed']}, verified={result['verified']}/{result['total_local_files']}"
                )
                result["error_code"] = "PartialFailure"
                logging.warning(result["msg"])

        except Exception as e:
            local_folder_path = str(local_folder)
            error_msg = f"❌ Error syncing folder {local_folder_path} -> s3://{bucket}/{prefix}: {str(e)}"
            logging.error(error_msg)
            result["success"] = False
            result["msg"] = error_msg
            result["error_code"] = type(e).__name__
            result["failed"] += 1
            result["errors"].append(error_msg)

        return result

    async def upload_folder(
            self,
            s3_folder_key: str,
            local_folder_path: str,
            bucket_name: Optional[str] = None,
            max_concurrent: int = 5,
            verify: bool = False,
            extra_args_provider: Optional[Callable[[str], Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Upload an entire folder to S3, preserving directory structure
        Either all files upload successfully or the operation fails (bullet proof)

        Args:
            s3_folder_key: S3 key (path) prefix where the folder will be uploaded (should end with "/")
            local_folder_path: Local directory path to upload
            bucket_name: S3 bucket name (uses default if not provided)
            max_concurrent: Maximum number of concurrent uploads (default: 5)

        Returns:
            Dict[str, Any]: Status information with success/failure counts and details
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return {**err, "total_files": 0, "uploaded_successfully": 0, "failed_uploads": 0, "errors": []}
        bucket = bucket_name or self.bucket_name

        # Validate local folder exists
        local_folder = Path(local_folder_path)
        if not local_folder.exists():
            return {
                "success": False,
                "msg": f"❌ Local folder not found: {local_folder_path}",
                "error_code": "FolderNotFound",
                "total_files": 0,
                "uploaded_successfully": 0,
                "failed_uploads": 0,
                "errors": []
            }

        if not local_folder.is_dir():
            return {
                "success": False,
                "msg": f"❌ Path is not a directory: {local_folder_path}",
                "error_code": "NotADirectory",
                "total_files": 0,
                "uploaded_successfully": 0,
                "failed_uploads": 0,
                "errors": []
            }

        # Ensure s3_folder_key normalized and ends with "/"
        s3_folder_key = self._normalize_key(s3_folder_key)
        if not s3_folder_key.endswith("/"):
            s3_folder_key += "/"

        result: Dict[str, Any] = {
            "success": True,
            "msg": "",
            "total_files": 0,
            "uploaded_successfully": 0,
            "failed_uploads": 0,
            "errors": [],
            "bucket": bucket,
            "s3_folder_key": s3_folder_key,
            "local_folder_path": local_folder_path
        }

        try:
            # Find all files in the local folder recursively
            all_files = []
            for file_path in local_folder.rglob("*"):
                if file_path.is_file():
                    # Get relative path from the base folder
                    relative_path = file_path.relative_to(local_folder)
                    # Convert Windows paths to forward slashes for S3
                    s3_key = s3_folder_key + str(relative_path).replace("\\", "/")
                    all_files.append({
                        "local_path": str(file_path),
                        "s3_key": s3_key,
                        "relative_path": str(relative_path)
                    })

            result["total_files"] = len(all_files)
            
            if result["total_files"] == 0:
                result["msg"] = f"✅ No files found in {local_folder_path} to upload"
                logging.info(result["msg"])
                return result

            logging.info(f"Found {result['total_files']} files to upload from {local_folder_path} to s3://{bucket}/{s3_folder_key}")

            # Upload files concurrently with semaphore to limit concurrent operations
            semaphore = asyncio.Semaphore(max_concurrent)

            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                async def _upload_single_file_with_semaphore(file_info: Dict[str, str]) -> Dict[str, Any]:
                    """Upload a single file with semaphore control using shared client"""
                    async with semaphore:
                        try:
                            # Infer content type
                            guess, _ = mimetypes.guess_type(file_info["local_path"])
                            # Start with provider-supplied args (if any) so they can override guesses
                            provider_args: Dict[str, Any] = {}
                            if extra_args_provider:
                                try:
                                    provider_args = extra_args_provider(file_info["relative_path"]) or {}
                                except Exception:
                                    provider_args = {}
                            extra_args: Dict[str, Any] = dict(provider_args)
                            if "ContentType" not in extra_args and guess:
                                extra_args["ContentType"] = guess
                            await s3.upload_file(
                                file_info["local_path"],
                                bucket,
                                file_info["s3_key"],
                                ExtraArgs=extra_args,
                                Config=self.transfer_config
                            )
                            return {
                                "file_info": file_info,
                                "result": {
                                    "success": True,
                                    "msg": f"Uploaded {file_info['relative_path']}"
                                }
                            }
                        except Exception as e:
                            return {
                                "file_info": file_info,
                                "result": {
                                    "success": False,
                                    "msg": f"Exception during upload: {str(e)}",
                                    "error_code": type(e).__name__
                                }
                            }

                # Execute all uploads concurrently
                upload_tasks = [_upload_single_file_with_semaphore(file_info) for file_info in all_files]
                upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

            # Process results
            for upload_result in upload_results:
                if isinstance(upload_result, Exception):
                    result["failed_uploads"] += 1
                    error_msg = f"Upload task failed with exception: {str(upload_result)}"
                    result["errors"].append(error_msg)
                    logging.error(error_msg)
                    continue

                file_info = upload_result["file_info"]
                upload_outcome = upload_result["result"]
                
                if upload_outcome.get("success", False):
                    result["uploaded_successfully"] += 1
                    logging.debug(f"Successfully uploaded {file_info['relative_path']}")
                else:
                    result["failed_uploads"] += 1
                    error_msg = f"Failed to upload {file_info['relative_path']}: {upload_outcome.get('msg', 'Unknown error')}"
                    result["errors"].append(error_msg)
                    logging.error(error_msg)

            # Determine final success status - bullet proof: all or nothing
            result["success"] = result["failed_uploads"] == 0
            
            if result["success"]:
                result["msg"] = f"✅ Successfully uploaded folder {local_folder_path} to s3://{bucket}/{s3_folder_key} ({result['uploaded_successfully']} files)"
                logging.info(result["msg"])
            else:
                result["msg"] = f"❌ Folder upload failed with {result['failed_uploads']} failures. Uploaded {result['uploaded_successfully']}/{result['total_files']} files"
                result["error_code"] = "PartialFailure"
                logging.error(result["msg"])

        except Exception as e:
            error_msg = f"❌ Error uploading folder {local_folder_path} to s3://{bucket}/{s3_folder_key}: {str(e)}"
            logging.error(error_msg)
            result["success"] = False
            result["msg"] = error_msg
            result["error_code"] = type(e).__name__
            result["errors"].append(error_msg)

        # Optional post-upload verification
        if verify:
            try:
                verification = await self.verify_folder_sync(
                    s3_folder_key=s3_folder_key,
                    local_folder_path=local_folder_path,
                    bucket_name=bucket
                )
                result["verification"] = verification
                if not verification.get("success", False):
                    result["success"] = False
                    result["error_code"] = "VerificationFailed"
                    result["msg"] = f"❌ Uploaded with verification mismatches: {verification.get('summary', verification.get('msg', 'Mismatch found'))}"
                    logging.error(result["msg"])
            except Exception as ve:
                # Do not fail the main operation if verification step errors; report it
                ver_msg = f"⚠️ Verification step failed: {str(ve)}"
                logging.warning(ver_msg)
                result["verification"] = {
                    "success": False,
                    "msg": ver_msg,
                    "error_code": type(ve).__name__
                }

        return result

    async def object_exists(
            self,
            s3_key: str,
            bucket_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Check if an object exists in S3

        Args:
            s3_key: S3 key (path) of the file to check
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            Dict with "success", "msg", "exists", "s3_key", "bucket", and optional "error_code"
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return {**err, "exists": False}
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key) or ""

        async def _exists_operation() -> Dict[str, Any]:
            try:
                async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                    await s3.head_object(Bucket=bucket, Key=norm_key)
                    return {
                        "success": True,
                        "msg": f"✅ Object s3://{bucket}/{norm_key} exists",
                        "exists": True,
                        "s3_key": norm_key,
                        "bucket": bucket
                    }
            except ClientError as e:
                err = e.response.get("Error", {}) if hasattr(e, "response") else {}
                error_code = err.get("Code")
                # Treat not-found variants as non-error negative existence
                if error_code in ("NoSuchKey", "NotFound", "404"):
                    return {
                        "success": True,
                        "msg": f"✅ Object s3://{bucket}/{norm_key} does not exist",
                        "exists": False,
                        "s3_key": norm_key,
                        "bucket": bucket
                    }
                else:
                    # Re-raise for retry mechanism to handle
                    raise

        return await self._retry_operation(
            _exists_operation,
            f"object_exists(s3://{bucket}/{norm_key})"
        )

    async def get_download_link(
            self,
            s3_key: str,
            bucket_name: Optional[str] = None,
            expires_in: int = 3600,
            as_attachment: bool = False,
            filename: Optional[str] = None,
            response_content_type: Optional[str] = None,
            content_disposition: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a direct (pre-signed) download link for an S3 object.

        Args:
            s3_key: S3 key (path) of the file
            bucket_name: S3 bucket name (uses default if not provided)
            expires_in: Link expiration in seconds (default 3600 = 1 hour)
            as_attachment: If True, sets Content-Disposition to attachment with a filename so browsers download
            filename: Optional filename to suggest to the browser; defaults to the object's basename
            response_content_type: Optional content type to force in the download response

        Returns:
            Dict with fields including success, msg, url, filename, bucket, s3_key, expires_in,
            content_type, content_length, etag, last_modified, endpoint_url; and error_code on failure
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key)

        async def _op() -> Dict[str, Any]:
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                # Ensure object exists and get basic metadata
                head = await s3.head_object(Bucket=bucket, Key=norm_key)
                content_length = int(head.get("ContentLength", 0))
                object_content_type = head.get("ContentType")
                last_modified = head.get("LastModified")
                etag_raw = head.get("ETag")
                etag = etag_raw.strip('"') if isinstance(etag_raw, str) else None

                # Determine filename to present to client
                default_filename = Path(norm_key).name
                use_filename = filename or head.get("Metadata", {}).get("filename") or default_filename

                # Decide response content-type
                chosen_resp_content_type = response_content_type
                if not chosen_resp_content_type:
                    guess, _ = mimetypes.guess_type(use_filename)
                    if guess:
                        chosen_resp_content_type = guess
                    elif object_content_type:
                        chosen_resp_content_type = object_content_type

                params: Dict[str, Any] = {
                    "Bucket": bucket,
                    "Key": norm_key,
                }
                # Response header overrides
                if content_disposition:
                    params["ResponseContentDisposition"] = content_disposition
                elif as_attachment and use_filename:
                    # RFC 5987 UTF-8 filename
                    params["ResponseContentDisposition"] = f"attachment; filename*=UTF-8''{quote(use_filename)}"
                if chosen_resp_content_type:
                    params["ResponseContentType"] = chosen_resp_content_type

                url = await s3.generate_presigned_url(
                    "get_object",
                    Params=params,
                    ExpiresIn=expires_in
                )
                return {
                    "success": True,
                    "msg": f"✅ Generated download link for s3://{bucket}/{norm_key}",
                    "url": url,
                    "bucket": bucket,
                    "s3_key": norm_key,
                    "filename": use_filename,
                    "expires_in": expires_in,
                    "object_content_type": object_content_type,
                    "response_content_type": chosen_resp_content_type,
                    "content_length": content_length,
                    "etag": etag,
                    "last_modified": str(last_modified) if last_modified is not None else None,
                    "endpoint_url": self.endpoint_url,
                }

        return await self._retry_operation(
            _op,
            f"get_download_link(s3://{bucket}/{norm_key})"
        )

    async def get_folder_download_links(
            self,
            s3_folder_key: str,
            bucket_name: Optional[str] = None,
            expires_in: int = 3600,
            as_attachment: bool = False,
            response_content_type: Optional[str] = None,
            max_keys: int = 1000,
            recursive: bool = True
    ) -> Dict[str, Any]:
        """
        Generate pre-signed download links for all files under an S3 folder prefix.

        Args:
            s3_folder_key: S3 folder prefix (e.g. "photos/2024/")
            bucket_name: S3 bucket name (uses default if not provided)
            expires_in: Link expiration in seconds (default 3600 = 1 hour)
            as_attachment: If True, sets Content-Disposition to attachment for each link
            response_content_type: Optional content type to force for all links
            max_keys: Maximum number of objects to process
            recursive: If True, include files in subfolders

        Returns:
            Dict with success, msg, links (list of dicts with url, s3_key, filename, content_type, content_length), count
        """
        list_result = await self.list_objects(
            prefix=s3_folder_key,
            bucket_name=bucket_name,
            max_keys=max_keys,
            recursive=recursive
        )
        if not list_result.get("success"):
            return list_result

        objects = list_result.get("objects", [])
        if not objects:
            bucket = bucket_name or self.bucket_name
            norm_key = self._normalize_key(s3_folder_key)
            return {
                "success": True,
                "msg": f"No files found under s3://{bucket}/{norm_key}",
                "links": [],
                "count": 0
            }

        links = []
        errors = []
        for obj in objects:
            result = await self.get_download_link(
                s3_key=obj["Key"],
                bucket_name=bucket_name,
                expires_in=expires_in,
                as_attachment=as_attachment,
                response_content_type=response_content_type
            )
            if result.get("success"):
                links.append({
                    "url": result["url"],
                    "s3_key": result["s3_key"],
                    "filename": result["filename"],
                    "content_type": result.get("response_content_type"),
                    "content_length": result.get("content_length"),
                })
            else:
                errors.append({"s3_key": obj["Key"], "msg": result.get("msg", "unknown error")})

        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_folder_key)
        result = {
            "success": True,
            "msg": f"Generated {len(links)} download links for s3://{bucket}/{norm_key}",
            "links": links,
            "count": len(links),
        }
        if errors:
            result["msg"] += f" ({len(errors)} failed)"
            result["errors"] = errors
        return result

    async def verify_folder_sync(
            self,
            s3_folder_key: str,
            local_folder_path: str,
            bucket_name: Optional[str] = None,
            fail_fast: bool = False
    ) -> Dict[str, Any]:
        """
        Verify that the files in a local folder and the files in an S3 folder (prefix) are in sync.
        The verification checks that:
        - Every local file exists in S3 under the given prefix
        - Every S3 object (non-folder marker) exists locally under the given folder
        - File sizes match between local and S3

        Args:
            s3_folder_key: S3 key (path) prefix (should end with "/")
            local_folder_path: Local directory path
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            Dict with success flag, counts, missing lists, mismatches, and a human-readable summary
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name

        # Normalize inputs
        s3_folder_key = self._normalize_key(s3_folder_key)
        if not s3_folder_key.endswith("/"):
            s3_folder_key += "/"

        local_folder = Path(local_folder_path)
        if not local_folder.exists() or not local_folder.is_dir():
            return {
                "success": False,
                "msg": f"❌ Local folder for verification is invalid: {local_folder_path}",
                "error_code": "InvalidLocalFolder"
            }

        try:
            # Build local files map: relative_path (with forward slashes) -> size
            local_map: Dict[str, int] = {}
            for file_path in local_folder.rglob("*"):
                if file_path.is_file():
                    rel = file_path.relative_to(local_folder)
                    rel_norm = str(rel).replace("\\", "/")
                    local_map[rel_norm] = file_path.stat().st_size

            # Build remote files map by listing all objects under prefix
            remote_map: Dict[str, int] = {}
            continuation_token = None
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                while True:
                    params: Dict[str, Any] = {
                        "Bucket": bucket,
                        "Prefix": s3_folder_key,
                        "MaxKeys": 1000
                    }
                    if continuation_token:
                        params["ContinuationToken"] = continuation_token

                    response = await s3.list_objects_v2(**params)
                    contents = response.get("Contents", [])
                    for obj in contents:
                        key = obj["Key"]
                        if key.endswith("/"):
                            continue  # skip folder markers
                        rel_key = key[len(s3_folder_key):]
                        # Normalize rel_key to forward slashes
                        rel_key = rel_key.replace("\\", "/")
                        remote_map[rel_key] = obj.get("Size", 0)

                    if not response.get("IsTruncated", False):
                        break
                    continuation_token = response.get("NextContinuationToken")

            # Compare sets
            local_set = set(local_map.keys())
            remote_set = set(remote_map.keys())

            missing_in_remote = sorted(list(local_set - remote_set))
            missing_in_local = sorted(list(remote_set - local_set))

            if fail_fast and (missing_in_remote or missing_in_local):
                summary_parts = []
                if missing_in_remote:
                    summary_parts.append(f"{len(missing_in_remote)} missing in remote")
                if missing_in_local:
                    summary_parts.append(f"{len(missing_in_local)} missing in local")
                summary = ", ".join(summary_parts)
                return {
                    "success": False,
                    "msg": f"❌ Verification failed: {summary}",
                    "summary": summary,
                    "bucket": bucket,
                    "s3_folder_key": s3_folder_key,
                    "local_folder_path": local_folder_path,
                    "total_local_files": len(local_set),
                    "total_remote_files": len(remote_set),
                    "matched_files": 0,
                    "missing_in_remote": missing_in_remote,
                    "missing_in_local": missing_in_local,
                    "size_mismatches": []
                }

            # Size mismatches for files present on both sides
            size_mismatches = []
            for rel in sorted(local_set & remote_set):
                lsize = local_map.get(rel, -1)
                rsize = remote_map.get(rel, -1)
                if lsize != rsize:
                    size_mismatches.append({
                        "relative_path": rel,
                        "local_size": lsize,
                        "remote_size": rsize
                    })
                    if fail_fast:
                        break

            success = len(missing_in_remote) == 0 and len(missing_in_local) == 0 and len(size_mismatches) == 0

            summary_parts = []
            if missing_in_remote:
                summary_parts.append(f"{len(missing_in_remote)} missing in remote")
            if missing_in_local:
                summary_parts.append(f"{len(missing_in_local)} missing in local")
            if size_mismatches:
                summary_parts.append(f"{len(size_mismatches)} size mismatches")
            summary = ", ".join(summary_parts) if summary_parts else "All files are in sync"

            result: Dict[str, Any] = {
                "success": success,
                "msg": f"✅ Verification passed: {summary}" if success else f"❌ Verification failed: {summary}",
                "summary": summary,
                "bucket": bucket,
                "s3_folder_key": s3_folder_key,
                "local_folder_path": local_folder_path,
                "total_local_files": len(local_set),
                "total_remote_files": len(remote_set),
                "matched_files": len(local_set & remote_set) - len(size_mismatches),
                "missing_in_remote": missing_in_remote,
                "missing_in_local": missing_in_local,
                "size_mismatches": size_mismatches
            }

            return result
        except Exception as e:
            error_msg = f"❌ Verification error for folder {local_folder_path} <-> s3://{bucket}/{s3_folder_key}: {str(e)}"
            logging.error(error_msg)
            return {
                "success": False,
                "msg": error_msg,
                "error_code": type(e).__name__
            }


    async def put_bytes(self, data: bytes, s3_key: str, bucket_name: Optional[str] = None, content_type: str = "application/octet-stream", metadata: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Upload small in-memory bytes as an object using put_object."""
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key)
        async def _op() -> Dict[str, Any]:
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                params: Dict[str, Any] = {
                    "Bucket": bucket,
                    "Key": norm_key,
                    "Body": data,
                    "ContentType": content_type or "application/octet-stream"
                }
                if metadata:
                    params["Metadata"] = metadata
                await s3.put_object(**params)
                return {
                    "success": True,
                    "msg": f"✅ Uploaded bytes to s3://{bucket}/{norm_key}",
                    "bucket": bucket,
                    "s3_key": norm_key,
                    "content_type": params["ContentType"]
                }
        return await self._retry_operation(_op, f"put_bytes(len={len(data)} -> s3://{bucket}/{norm_key})")

    async def put_text(self, text: str, s3_key: str, bucket_name: Optional[str] = None, content_type: str = "text/plain; charset=utf-8", metadata: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        return await self.put_bytes(text.encode("utf-8"), s3_key, bucket_name, content_type, metadata)

    async def get_text(self, s3_key: str, bucket_name: Optional[str] = None, encoding: str = "utf-8") -> Dict[str, Any]:
        """Download object content as text using get_object."""
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name
        norm_key = self._normalize_key(s3_key)
        async def _op() -> Dict[str, Any]:
            async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                resp = await s3.get_object(Bucket=bucket, Key=norm_key)
                stream = resp["Body"]
                try:
                    body = await stream.read()
                finally:
                    try:
                        await stream.close()
                    except Exception:
                        pass
                return {
                    "success": True,
                    "msg": f"✅ Downloaded text from s3://{bucket}/{norm_key}",
                    "bucket": bucket,
                    "s3_key": norm_key,
                    "text": body.decode(encoding)
                }
        return await self._retry_operation(_op, f"get_text(s3://{bucket}/{norm_key})")

    async def verify_file(
            self,
            local_file_path: str,
            s3_key: str,
            bucket_name: Optional[str] = None,
            use_md5: bool = False,
            use_sha256: bool = False
    ) -> Dict[str, Any]:
        """
        Verify a single local file against a cloud (S3) object.
        Checks existence and size. Optionally verifies MD5 when ETag is a single-part MD5.

        Args:
            local_file_path: Path to the local file
            s3_key: S3 key (path) to compare with
            bucket_name: S3 bucket name (uses default if not provided)
            use_md5: If True and ETag is a simple MD5 (no "-") then compute local MD5 and compare

        Returns:
            Dict with fields: success, msg, bucket, s3_key, local_file, exists_remote,
            local_size, remote_size, sizes_match, etag, md5_checked, md5_match (optional),
            and error_code on failure
        """
        err = self._validate_bucket(bucket_name)
        if err:
            return err
        bucket = bucket_name or self.bucket_name

        local_path = Path(local_file_path)
        if not local_path.exists() or not local_path.is_file():
            return {
                "success": False,
                "msg": f"❌ Local file not found: {local_file_path}",
                "error_code": "FileNotFound"
            }

        async def _md5_of_file(path: Path) -> str:
            import hashlib
            hash_md5 = hashlib.md5()
            # Use aiofiles to avoid blocking
            async with aiofiles.open(path, "rb") as f:  # type: ignore
                while True:
                    chunk = await f.read(1024 * 1024)
                    if not chunk:
                        break
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        async def _verify_operation() -> Dict[str, Any]:
            try:
                async with self._require_session().client("s3", endpoint_url=self.endpoint_url, config=self.config) as s3:
                    # Retrieve remote object's metadata
                    head = await s3.head_object(Bucket=bucket, Key=self._normalize_key(s3_key))
                    remote_size = int(head.get("ContentLength", 0))
                    etag_raw = head.get("ETag")
                    # ETag comes quoted, e.g. "abcd..."
                    etag = etag_raw.strip('"') if isinstance(etag_raw, str) else None
                    remote_sha256_b64 = head.get("ChecksumSHA256")

                    local_size = local_path.stat().st_size
                    sizes_match = (local_size == remote_size)

                    md5_checked = False
                    md5_match: Optional[bool] = None
                    md5_supported = False

                    # Only attempt MD5 compare when requested and ETag indicates single-part upload
                    if use_md5 and etag and "-" not in etag:
                        md5_supported = True
                        md5_checked = True
                        local_md5 = await _md5_of_file(local_path)
                        md5_match = (local_md5 == etag)

                    sha256_checked = False
                    sha256_match: Optional[bool] = None
                    if use_sha256 and remote_sha256_b64:
                        import hashlib, base64
                        sha256_checked = True
                        # Compute local sha256 streaming
                        h = hashlib.sha256()
                        async with aiofiles.open(local_path, "rb") as f:  # type: ignore
                            while True:
                                chunk = await f.read(1024 * 1024)
                                if not chunk:
                                    break
                                h.update(chunk)
                        local_sha256_b64 = base64.b64encode(h.digest()).decode("ascii")
                        sha256_match = (local_sha256_b64 == remote_sha256_b64)

                    success = sizes_match and (md5_match is not False) and (sha256_match is not False)

                    msg: str
                    if success:
                        parts = ["✅ File verified"]
                        if not sizes_match:
                            parts.append("(size)!")  # should not happen when success True
                        if md5_checked:
                            parts.append("(MD5 matched)") if md5_match else parts.append("(MD5 skipped)")
                        if sha256_checked:
                            parts.append("(SHA256 matched)") if sha256_match else parts.append("(SHA256 mismatch)")
                        msg = " ".join(parts) + f": {local_file_path} <-> s3://{bucket}/{s3_key}"
                    else:
                        reasons = []
                        if not sizes_match:
                            reasons.append("size mismatch")
                        if md5_checked and md5_match is False:
                            reasons.append("md5 mismatch")
                        if sha256_checked and sha256_match is False:
                            reasons.append("sha256 mismatch")
                        reason_str = ", ".join(reasons) if reasons else "unknown reason"
                        msg = f"❌ File verification failed ({reason_str}): {local_file_path} <-> s3://{bucket}/{s3_key}"

                    result: Dict[str, Any] = {
                        "success": success,
                        "msg": msg,
                        "bucket": bucket,
                        "s3_key": s3_key,
                        "local_file": str(local_path),
                        "exists_remote": True,
                        "local_size": local_size,
                        "remote_size": remote_size,
                        "sizes_match": sizes_match,
                        "etag": etag,
                        "md5_requested": use_md5,
                        "md5_supported": md5_supported,
                        "md5_checked": md5_checked,
                        "md5_match": md5_match,
                        "sha256_checked": sha256_checked,
                        "sha256_match": sha256_match
                    }
                    return result
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code in ("404", "NoSuchKey"):
                    return {
                        "success": False,
                        "msg": f"❌ File verification failed: remote object not found s3://{bucket}/{s3_key}",
                        "error_code": "NoSuchKey",
                        "bucket": bucket,
                        "s3_key": s3_key,
                        "local_file": str(local_path),
                        "exists_remote": False
                    }
                raise

        return await self._retry_operation(
            _verify_operation,
            f"verify_file({local_file_path} <-> s3://{bucket}/{s3_key})"
        )
