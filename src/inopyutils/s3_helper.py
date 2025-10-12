import asyncio
import aiofiles
from aioboto3 import Session
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError, ConnectionClosedError
from botocore.config import Config
from pathlib import Path
from typing import Optional, Dict, Any, Callable, Awaitable
import logging
import random
import mimetypes


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
            aws_access_key_id='your_b2_key_id',
            aws_secret_access_key='your_b2_application_key',
            endpoint_url='https://s3.us-west-000.backblazeb2.com',
            region_name='us-west-000',
            bucket_name='your-bucket-name'
        )
    """

    def __init__(
            self,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            aws_session_token: Optional[str] = None,
            region_name: str = 'us-east-1',
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
        self.config = config

        if aws_access_key_id and aws_secret_access_key:
            self.session = Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name
            )
        else:
            self.session = Session(region_name=region_name)

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
                # If operation returns unsuccessful result, don't retry
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
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code in ['NoSuchBucket', 'NoSuchKey', 'AccessDenied', 'InvalidAccessKeyId']:
                    error_msg = f"❌ {operation_name} failed with non-retryable client error {error_code}: {str(e)}"
                    logging.error(error_msg)
                    return {
                        "success": False,
                        "msg": error_msg,
                        "error_code": error_code
                    }
                
                last_exception = e
                if attempt < self.retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"{operation_name} attempt {attempt + 1} failed with {error_code}, retrying in {wait_time:.2f}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    error_msg = f"❌ {operation_name} failed after {self.retries + 1} attempts with client error {error_code}: {str(e)}"
                    logging.error(error_msg)
                    return {
                        "success": False,
                        "msg": error_msg,
                        "error_code": error_code
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
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket"
            }

        # Check if local file exists
        if not Path(local_file_path).exists():
            return {
                "success": False,
                "msg": f"❌ Local file not found: {local_file_path}",
                "error_code": "FileNotFound"
            }

        async def _upload_operation() -> Dict[str, Any]:
            async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                local_extra_args = dict(extra_args or {})
                if 'ContentType' not in local_extra_args:
                    guess, _ = mimetypes.guess_type(local_file_path)
                    if guess:
                        local_extra_args['ContentType'] = guess
                await s3.upload_file(
                    local_file_path,
                    bucket,
                    s3_key,
                    ExtraArgs=local_extra_args
                )
                success_msg = f"✅ Successfully uploaded {Path(local_file_path).name} to s3://{bucket}/{s3_key}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": s3_key,
                    "bucket": bucket,
                    "local_file": local_file_path
                }

        return await self._retry_operation(
            _upload_operation,
            f"upload_file({local_file_path} -> s3://{bucket}/{s3_key})"
        )

    async def upload_file_object(
            self,
            local_file_path: str,
            s3_key: str,
            bucket_name: Optional[str] = None,
            content_type: str = 'application/octet-stream',
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
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket"
            }

        # Check if local file exists
        if not Path(local_file_path).exists():
            return {
                "success": False,
                "msg": f"❌ Local file not found: {local_file_path}",
                "error_code": "FileNotFound"
            }

        async def _upload_operation() -> Dict[str, Any]:
            async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                # Build ExtraArgs with sensible defaults and optional overrides
                guess, _ = mimetypes.guess_type(local_file_path)
                effective_content_type = content_type
                if (not content_type or content_type == 'application/octet-stream') and guess:
                    effective_content_type = guess
                extra_args = {'ContentType': effective_content_type}
                if metadata:
                    extra_args['Metadata'] = metadata
                await s3.upload_file(
                    local_file_path,
                    bucket,
                    s3_key,
                    ExtraArgs=extra_args
                )

                success_msg = f"✅ Successfully uploaded {Path(local_file_path).name} to s3://{bucket}/{s3_key} with content type {effective_content_type}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": s3_key,
                    "bucket": bucket,
                    "content_type": effective_content_type,
                    "local_file": local_file_path,
                    "metadata": metadata or {}
                }

        return await self._retry_operation(
            _upload_operation,
            f"upload_file_object({local_file_path} -> s3://{bucket}/{s3_key})"
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
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket"
            }

        async def _download_operation() -> Dict[str, Any]:
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                await s3.download_file(bucket, s3_key, local_file_path)
                success_msg = f"✅ Successfully downloaded s3://{bucket}/{s3_key} to {local_file_path}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": s3_key,
                    "bucket": bucket,
                    "local_file": local_file_path
                }

        return await self._retry_operation(
            _download_operation,
            f"download_file(s3://{bucket}/{s3_key} -> {local_file_path})"
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
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket"
            }

        async def _download_operation() -> Dict[str, Any]:
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                response = await s3.get_object(Bucket=bucket, Key=s3_key)

                async with aiofiles.open(local_file_path, 'wb') as file:
                    async with response['Body'] as stream:
                        async for chunk in stream.iter_chunks():
                            await file.write(chunk)

                success_msg = f"✅ Successfully downloaded s3://{bucket}/{s3_key} to {local_file_path}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": s3_key,
                    "bucket": bucket,
                    "local_file": local_file_path
                }

        return await self._retry_operation(
            _download_operation,
            f"download_file_object(s3://{bucket}/{s3_key} -> {local_file_path})"
        )

    async def list_objects(
            self,
            prefix: str = "",
            bucket_name: Optional[str] = None,
            max_keys: int = 1000
    ) -> Dict[str, Any]:
        """
        List objects in S3 bucket

        Args:
            prefix: Filter objects by prefix
            bucket_name: S3 bucket name (uses default if not provided)
            max_keys: Maximum number of objects to return

        Returns:
            Dict with "success", "msg", "objects", "count", and optional "error_code"
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket",
                "objects": [],
                "count": 0
            }

        # Input validation
        if max_keys <= 0 or max_keys > 1000:
            return {
                "success": False,
                "msg": "❌ max_keys must be between 1 and 1000",
                "error_code": "InvalidParameter",
                "objects": [],
                "count": 0
            }

        async def _list_operation() -> Dict[str, Any]:
            async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                response = await s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=max_keys
                )

                objects = []
                if 'Contents' in response:
                    for obj in response['Contents']:
                        objects.append({
                            'Key': obj['Key'],
                            'Size': obj['Size'],
                            'LastModified': obj['LastModified'],
                            'ETag': obj['ETag']
                        })

                success_msg = f"✅ Found {len(objects)} objects in s3://{bucket} with prefix '{prefix}'"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "objects": objects,
                    "count": len(objects),
                    "bucket": bucket,
                    "prefix": prefix
                }

        return await self._retry_operation(
            _list_operation,
            f"list_objects(s3://{bucket}, prefix='{prefix}')"
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
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket"
            }

        async def _delete_operation() -> Dict[str, Any]:
            async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                await s3.delete_object(Bucket=bucket, Key=s3_key)
                success_msg = f"✅ Successfully deleted s3://{bucket}/{s3_key}"
                logging.info(success_msg)
                return {
                    "success": True,
                    "msg": success_msg,
                    "s3_key": s3_key,
                    "bucket": bucket
                }

        return await self._retry_operation(
            _delete_operation,
            f"delete_object(s3://{bucket}/{s3_key})"
        )

    async def download_folder(
            self,
            s3_folder_key: str,
            local_folder_path: str,
            bucket_name: Optional[str] = None,
            max_concurrent: int = 5,
            verify: bool = True
    ) -> Dict[str, Any]:
        """
        Download an entire folder from S3, preserving directory structure locally

        Args:
            s3_folder_key: S3 key (path) of the folder to download (should end with '/')
            local_folder_path: Local directory path where the folder will be saved
            bucket_name: S3 bucket name (uses default if not provided)
            max_concurrent: Maximum number of concurrent downloads (default: 5)

        Returns:
            Dict[str, Any]: Status information with success/failure counts and details
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket",
                "total_files": 0,
                "downloaded_successfully": 0,
                "failed_downloads": 0,
                "errors": []
            }

        if not s3_folder_key.endswith('/'):
            s3_folder_key += '/'

        local_folder = Path(local_folder_path)
        local_folder.mkdir(parents=True, exist_ok=True)

        result = {
            'success': True,
            'msg': "",
            'total_files': 0,
            'downloaded_successfully': 0,
            'failed_downloads': 0,
            'errors': [],
            'bucket': bucket,
            's3_folder_key': s3_folder_key,
            'local_folder_path': local_folder_path
        }

        try:
            all_objects = []
            continuation_token = None
            
            async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                while True:
                    list_params = {
                        'Bucket': bucket,
                        'Prefix': s3_folder_key,
                        'MaxKeys': 1000
                    }
                    
                    if continuation_token:
                        list_params['ContinuationToken'] = continuation_token

                    response = await s3.list_objects_v2(**list_params)

                    if 'Contents' in response:
                        all_objects.extend(response['Contents'])

                    if not response.get('IsTruncated', False):
                        break
                    
                    continuation_token = response.get('NextContinuationToken')

            result['total_files'] = len(all_objects)
            
            # Filter out directory markers (keys ending with '/')
            file_objects = [obj for obj in all_objects if not obj['Key'].endswith('/')]
            result['total_files'] = len(file_objects)
            
            logging.info(f"Found {result['total_files']} files to download from s3://{bucket}/{s3_folder_key}")

            # Download files concurrently with semaphore to limit concurrent operations
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def _download_single_file_with_semaphore(obj: Dict[str, Any]) -> Dict[str, Any]:
                """Download a single file with semaphore control"""
                async with semaphore:
                    s3_key = obj['Key']
                    relative_path = s3_key[len(s3_folder_key):]
                    local_file_path = local_folder / relative_path
                    
                    try:
                        download_result = await self.download_file(s3_key, str(local_file_path), bucket_name)
                        return {
                            's3_key': s3_key,
                            'relative_path': relative_path,
                            'result': download_result
                        }
                    except Exception as e:
                        return {
                            's3_key': s3_key,
                            'relative_path': relative_path,
                            'result': {
                                'success': False,
                                'msg': f"Exception during download: {str(e)}",
                                'error_code': type(e).__name__
                            }
                        }

            # Execute all downloads concurrently
            download_tasks = [_download_single_file_with_semaphore(obj) for obj in file_objects]
            download_results = await asyncio.gather(*download_tasks, return_exceptions=True)

            # Process results
            for download_result in download_results:
                if isinstance(download_result, Exception):
                    result['failed_downloads'] += 1
                    error_msg = f"Download task failed with exception: {str(download_result)}"
                    result['errors'].append(error_msg)
                    logging.error(error_msg)
                    continue

                s3_key = download_result['s3_key']
                relative_path = download_result['relative_path']
                download_outcome = download_result['result']
                
                if download_outcome.get('success', False):
                    result['downloaded_successfully'] += 1
                    logging.debug(f"Successfully downloaded {relative_path}")
                else:
                    result['failed_downloads'] += 1
                    error_msg = f"Failed to download {relative_path}: {download_outcome.get('msg', 'Unknown error')}"
                    result['errors'].append(error_msg)
                    logging.error(error_msg)

            result['success'] = result['failed_downloads'] == 0
            
            if result['success']:
                result['msg'] = f"✅ Successfully downloaded folder s3://{bucket}/{s3_folder_key} to {local_folder_path} ({result['downloaded_successfully']} files)"
                logging.info(result['msg'])
            else:
                result['msg'] = f"❌ Folder download completed with {result['failed_downloads']} failures. Downloaded {result['downloaded_successfully']}/{result['total_files']} files"
                result['error_code'] = "PartialFailure"
                logging.warning(result['msg'])

        except Exception as e:
            error_msg = f"❌ Error downloading folder s3://{bucket}/{s3_folder_key}: {str(e)}"
            logging.error(error_msg)
            result['success'] = False
            result['msg'] = error_msg
            result['error_code'] = type(e).__name__
            result['errors'].append(error_msg)

        # Optional post-download verification
        if verify:
            try:
                verification = await self.verify_folder_sync(
                    s3_folder_key=s3_folder_key,
                    local_folder_path=local_folder_path,
                    bucket_name=bucket
                )
                result['verification'] = verification
                if not verification.get('success', False):
                    result['success'] = False
                    result['error_code'] = 'VerificationFailed'
                    result['msg'] = f"❌ Downloaded with verification mismatches: {verification.get('summary', verification.get('msg', 'Mismatch found'))}"
                    logging.error(result['msg'])
            except Exception as ve:
                # Do not fail the main operation if verification step errors; report it
                ver_msg = f"⚠️ Verification step failed: {str(ve)}"
                logging.warning(ver_msg)
                result['verification'] = {
                    'success': False,
                    'msg': ver_msg,
                    'error_code': type(ve).__name__
                }

        return result

    async def upload_folder(
            self,
            s3_folder_key: str,
            local_folder_path: str,
            bucket_name: Optional[str] = None,
            max_concurrent: int = 5,
            verify: bool = True
    ) -> Dict[str, Any]:
        """
        Upload an entire folder to S3, preserving directory structure
        Either all files upload successfully or the operation fails (bullet proof)

        Args:
            s3_folder_key: S3 key (path) prefix where the folder will be uploaded (should end with '/')
            local_folder_path: Local directory path to upload
            bucket_name: S3 bucket name (uses default if not provided)
            max_concurrent: Maximum number of concurrent uploads (default: 5)

        Returns:
            Dict[str, Any]: Status information with success/failure counts and details
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket",
                "total_files": 0,
                "uploaded_successfully": 0,
                "failed_uploads": 0,
                "errors": []
            }

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

        # Ensure s3_folder_key ends with '/'
        if not s3_folder_key.endswith('/'):
            s3_folder_key += '/'

        result = {
            'success': True,
            'msg': "",
            'total_files': 0,
            'uploaded_successfully': 0,
            'failed_uploads': 0,
            'errors': [],
            'bucket': bucket,
            's3_folder_key': s3_folder_key,
            'local_folder_path': local_folder_path
        }

        try:
            # Find all files in the local folder recursively
            all_files = []
            for file_path in local_folder.rglob('*'):
                if file_path.is_file():
                    # Get relative path from the base folder
                    relative_path = file_path.relative_to(local_folder)
                    # Convert Windows paths to forward slashes for S3
                    s3_key = s3_folder_key + str(relative_path).replace('\\', '/')
                    all_files.append({
                        'local_path': str(file_path),
                        's3_key': s3_key,
                        'relative_path': str(relative_path)
                    })

            result['total_files'] = len(all_files)
            
            if result['total_files'] == 0:
                result['msg'] = f"✅ No files found in {local_folder_path} to upload"
                logging.info(result['msg'])
                return result

            logging.info(f"Found {result['total_files']} files to upload from {local_folder_path} to s3://{bucket}/{s3_folder_key}")

            # Upload files concurrently with semaphore to limit concurrent operations
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def _upload_single_file_with_semaphore(file_info: Dict[str, str]) -> Dict[str, Any]:
                """Upload a single file with semaphore control"""
                async with semaphore:
                    try:
                        upload_result = await self.upload_file(
                            local_file_path=file_info['local_path'],
                            s3_key=file_info['s3_key'],
                            bucket_name=bucket
                        )
                        return {
                            'file_info': file_info,
                            'result': upload_result
                        }
                    except Exception as e:
                        return {
                            'file_info': file_info,
                            'result': {
                                'success': False,
                                'msg': f"Exception during upload: {str(e)}",
                                'error_code': type(e).__name__
                            }
                        }

            # Execute all uploads concurrently
            upload_tasks = [_upload_single_file_with_semaphore(file_info) for file_info in all_files]
            upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

            # Process results
            for upload_result in upload_results:
                if isinstance(upload_result, Exception):
                    result['failed_uploads'] += 1
                    error_msg = f"Upload task failed with exception: {str(upload_result)}"
                    result['errors'].append(error_msg)
                    logging.error(error_msg)
                    continue

                file_info = upload_result['file_info']
                upload_outcome = upload_result['result']
                
                if upload_outcome.get('success', False):
                    result['uploaded_successfully'] += 1
                    logging.debug(f"Successfully uploaded {file_info['relative_path']}")
                else:
                    result['failed_uploads'] += 1
                    error_msg = f"Failed to upload {file_info['relative_path']}: {upload_outcome.get('msg', 'Unknown error')}"
                    result['errors'].append(error_msg)
                    logging.error(error_msg)

            # Determine final success status - bullet proof: all or nothing
            result['success'] = result['failed_uploads'] == 0
            
            if result['success']:
                result['msg'] = f"✅ Successfully uploaded folder {local_folder_path} to s3://{bucket}/{s3_folder_key} ({result['uploaded_successfully']} files)"
                logging.info(result['msg'])
            else:
                result['msg'] = f"❌ Folder upload failed with {result['failed_uploads']} failures. Uploaded {result['uploaded_successfully']}/{result['total_files']} files"
                result['error_code'] = "PartialFailure"
                logging.error(result['msg'])

        except Exception as e:
            error_msg = f"❌ Error uploading folder {local_folder_path} to s3://{bucket}/{s3_folder_key}: {str(e)}"
            logging.error(error_msg)
            result['success'] = False
            result['msg'] = error_msg
            result['error_code'] = type(e).__name__
            result['errors'].append(error_msg)

        # Optional post-upload verification
        if verify:
            try:
                verification = await self.verify_folder_sync(
                    s3_folder_key=s3_folder_key,
                    local_folder_path=local_folder_path,
                    bucket_name=bucket
                )
                result['verification'] = verification
                if not verification.get('success', False):
                    result['success'] = False
                    result['error_code'] = 'VerificationFailed'
                    result['msg'] = f"❌ Uploaded with verification mismatches: {verification.get('summary', verification.get('msg', 'Mismatch found'))}"
                    logging.error(result['msg'])
            except Exception as ve:
                # Do not fail the main operation if verification step errors; report it
                ver_msg = f"⚠️ Verification step failed: {str(ve)}"
                logging.warning(ver_msg)
                result['verification'] = {
                    'success': False,
                    'msg': ver_msg,
                    'error_code': type(ve).__name__
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
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket",
                "exists": False
            }

        async def _exists_operation() -> Dict[str, Any]:
            try:
                async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                    await s3.head_object(Bucket=bucket, Key=s3_key)
                    return {
                        "success": True,
                        "msg": f"✅ Object s3://{bucket}/{s3_key} exists",
                        "exists": True,
                        "s3_key": s3_key,
                        "bucket": bucket
                    }
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'NoSuchKey' or error_code == '404':
                    return {
                        "success": True,
                        "msg": f"✅ Object s3://{bucket}/{s3_key} does not exist",
                        "exists": False,
                        "s3_key": s3_key,
                        "bucket": bucket
                    }
                else:
                    # Re-raise for retry mechanism to handle
                    raise

        return await self._retry_operation(
            _exists_operation,
            f"object_exists(s3://{bucket}/{s3_key})"
        )
    async def verify_folder_sync(
            self,
            s3_folder_key: str,
            local_folder_path: str,
            bucket_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Verify that the files in a local folder and the files in an S3 folder (prefix) are in sync.
        The verification checks that:
        - Every local file exists in S3 under the given prefix
        - Every S3 object (non-folder marker) exists locally under the given folder
        - File sizes match between local and S3

        Args:
            s3_folder_key: S3 key (path) prefix (should end with '/')
            local_folder_path: Local directory path
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            Dict with success flag, counts, missing lists, mismatches, and a human-readable summary
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket"
            }

        # Normalize inputs
        if not s3_folder_key.endswith('/'):
            s3_folder_key += '/'

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
            for file_path in local_folder.rglob('*'):
                if file_path.is_file():
                    rel = file_path.relative_to(local_folder)
                    rel_norm = str(rel).replace('\\', '/')
                    local_map[rel_norm] = file_path.stat().st_size

            # Build remote files map by listing all objects under prefix
            remote_map: Dict[str, int] = {}
            continuation_token = None
            async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                while True:
                    params: Dict[str, Any] = {
                        'Bucket': bucket,
                        'Prefix': s3_folder_key,
                        'MaxKeys': 1000
                    }
                    if continuation_token:
                        params['ContinuationToken'] = continuation_token

                    response = await s3.list_objects_v2(**params)
                    contents = response.get('Contents', [])
                    for obj in contents:
                        key = obj['Key']
                        if key.endswith('/'):
                            continue  # skip folder markers
                        rel_key = key[len(s3_folder_key):]
                        remote_map[rel_key] = obj.get('Size', 0)

                    if not response.get('IsTruncated', False):
                        break
                    continuation_token = response.get('NextContinuationToken')

            # Compare sets
            local_set = set(local_map.keys())
            remote_set = set(remote_map.keys())

            missing_in_remote = sorted(list(local_set - remote_set))
            missing_in_local = sorted(list(remote_set - local_set))

            # Size mismatches for files present on both sides
            size_mismatches = []
            for rel in sorted(local_set & remote_set):
                lsize = local_map.get(rel, -1)
                rsize = remote_map.get(rel, -1)
                if lsize != rsize:
                    size_mismatches.append({
                        'relative_path': rel,
                        'local_size': lsize,
                        'remote_size': rsize
                    })

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
                'success': success,
                'msg': f"✅ Verification passed: {summary}" if success else f"❌ Verification failed: {summary}",
                'summary': summary,
                'bucket': bucket,
                's3_folder_key': s3_folder_key,
                'local_folder_path': local_folder_path,
                'total_local_files': len(local_set),
                'total_remote_files': len(remote_set),
                'matched_files': len(local_set & remote_set) - len(size_mismatches),
                'missing_in_remote': missing_in_remote,
                'missing_in_local': missing_in_local,
                'size_mismatches': size_mismatches
            }

            return result
        except Exception as e:
            error_msg = f"❌ Verification error for folder {local_folder_path} <-> s3://{bucket}/{s3_folder_key}: {str(e)}"
            logging.error(error_msg)
            return {
                'success': False,
                'msg': error_msg,
                'error_code': type(e).__name__
            }


    async def verify_file(
            self,
            local_file_path: str,
            s3_key: str,
            bucket_name: Optional[str] = None,
            use_md5: bool = False
    ) -> Dict[str, Any]:
        """
        Verify a single local file against a cloud (S3) object.
        Checks existence and size. Optionally verifies MD5 when ETag is a single-part MD5.

        Args:
            local_file_path: Path to the local file
            s3_key: S3 key (path) to compare with
            bucket_name: S3 bucket name (uses default if not provided)
            use_md5: If True and ETag is a simple MD5 (no '-') then compute local MD5 and compare

        Returns:
            Dict with fields: success, msg, bucket, s3_key, local_file, exists_remote,
            local_size, remote_size, sizes_match, etag, md5_checked, md5_match (optional),
            and error_code on failure
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            return {
                "success": False,
                "msg": "❌ Bucket name must be provided either during initialization or method call",
                "error_code": "MissingBucket"
            }

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
            async with aiofiles.open(path, 'rb') as f:  # type: ignore
                while True:
                    chunk = await f.read(1024 * 1024)
                    if not chunk:
                        break
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        async def _verify_operation() -> Dict[str, Any]:
            try:
                async with self.session.client('s3', endpoint_url=self.endpoint_url, config=self.config) as s3:
                    # Retrieve remote object's metadata
                    head = await s3.head_object(Bucket=bucket, Key=s3_key)
                    remote_size = int(head.get('ContentLength', 0))
                    etag_raw = head.get('ETag')
                    # ETag comes quoted, e.g. "abcd..."
                    etag = etag_raw.strip('"') if isinstance(etag_raw, str) else None

                    local_size = local_path.stat().st_size
                    sizes_match = (local_size == remote_size)

                    md5_checked = False
                    md5_match: Optional[bool] = None
                    md5_supported = False

                    # Only attempt MD5 compare when requested and ETag indicates single-part upload
                    if use_md5 and etag and '-' not in etag:
                        md5_supported = True
                        md5_checked = True
                        local_md5 = await _md5_of_file(local_path)
                        md5_match = (local_md5 == etag)

                    success = sizes_match and (md5_match is not False)

                    msg: str
                    if success:
                        parts = ["✅ File verified"]
                        if not sizes_match:
                            parts.append("(size)!")  # should not happen when success True
                        if md5_checked:
                            parts.append("(MD5 matched)") if md5_match else parts.append("(MD5 skipped)")
                        msg = " ".join(parts) + f": {local_file_path} <-> s3://{bucket}/{s3_key}"
                    else:
                        reasons = []
                        if not sizes_match:
                            reasons.append("size mismatch")
                        if md5_checked and md5_match is False:
                            reasons.append("md5 mismatch")
                        msg = f"❌ File verification failed ({', '.join(reasons) if reasons else 'unknown reason'}): {local_file_path} <-> s3://{bucket}/{s3_key}"

                    result: Dict[str, Any] = {
                        'success': success,
                        'msg': msg,
                        'bucket': bucket,
                        's3_key': s3_key,
                        'local_file': str(local_path),
                        'exists_remote': True,
                        'local_size': local_size,
                        'remote_size': remote_size,
                        'sizes_match': sizes_match,
                        'etag': etag,
                        'md5_requested': use_md5,
                        'md5_supported': md5_supported,
                        'md5_checked': md5_checked,
                        'md5_match': md5_match
                    }
                    return result
            except ClientError as e:
                code = e.response.get('Error', {}).get('Code')
                if code in ('404', 'NoSuchKey'):
                    return {
                        'success': False,
                        'msg': f"❌ File verification failed: remote object not found s3://{bucket}/{s3_key}",
                        'error_code': 'NoSuchKey',
                        'bucket': bucket,
                        's3_key': s3_key,
                        'local_file': str(local_path),
                        'exists_remote': False
                    }
                raise

        return await self._retry_operation(
            _verify_operation,
            f"verify_file({local_file_path} <-> s3://{bucket}/{s3_key})"
        )
