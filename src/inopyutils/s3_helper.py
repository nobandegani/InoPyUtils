import asyncio
import aiofiles
from aioboto3 import Session
from botocore.exceptions import ClientError, NoCredentialsError
from pathlib import Path
from typing import Optional, Dict, Any, Callable, Awaitable
import logging
import random


class InoS3Helper:
    """
    Async S3 client class that wraps aiboto3 functionality
    
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
            retries: int = 3
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
        """
        self.region_name = region_name
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.retries = retries

        if aws_access_key_id and aws_secret_access_key:
            self.session = Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name
            )
        else:
            self.session = Session(region_name=region_name)

    async def _retry_operation(
            self,
            operation: Callable[[], Awaitable[bool]],
            operation_name: str
    ) -> bool:
        """
        Retry an operation with exponential backoff
        
        Args:
            operation: Async function to retry
            operation_name: Name of the operation for logging
            
        Returns:
            bool: True if operation succeeded, False if all retries failed
        """
        last_exception = None
        
        for attempt in range(self.retries + 1):  # +1 for initial attempt
            try:
                return await operation()
            except (FileNotFoundError, NoCredentialsError, ValueError) as e:
                logging.error(f"{operation_name} failed with non-retryable error: {str(e)}")
                return False
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code in ['NoSuchBucket', 'NoSuchKey', 'AccessDenied', 'InvalidAccessKeyId']:
                    logging.error(f"{operation_name} failed with non-retryable client error {error_code}: {str(e)}")
                    return False
                
                last_exception = e
                if attempt < self.retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"{operation_name} attempt {attempt + 1} failed with {error_code}, retrying in {wait_time:.2f}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(f"{operation_name} failed after {self.retries + 1} attempts with client error {error_code}: {str(e)}")
            except Exception as e:
                last_exception = e
                if attempt < self.retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"{operation_name} attempt {attempt + 1} failed, retrying in {wait_time:.2f}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(f"{operation_name} failed after {self.retries + 1} attempts: {str(e)}")
        
        return False

    async def upload_file(
            self,
            local_file_path: str,
            s3_key: str,
            bucket_name: Optional[str] = None,
            extra_args: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Upload a file to S3 with automatic retry on failure

        Args:
            local_file_path: Path to the local file to upload
            s3_key: S3 key (path) where the file will be stored
            bucket_name: S3 bucket name (uses default if not provided)
            extra_args: Extra arguments for the upload (e.g., metadata, ACL)

        Returns:
            bool: True if upload successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be provided either during initialization or method call")

        async def _upload_operation() -> bool:
            async with self.session.client('s3', endpoint_url=self.endpoint_url) as s3:
                await s3.upload_file(
                    local_file_path,
                    bucket,
                    s3_key,
                    ExtraArgs=extra_args or {}
                )
                logging.info(f"Successfully uploaded {local_file_path} to s3://{bucket}/{s3_key}")
                return True

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
    ) -> bool:
        """
        Upload a file using put_object for more control over metadata with automatic retry on failure

        Args:
            local_file_path: Path to the local file to upload
            s3_key: S3 key (path) where the file will be stored
            bucket_name: S3 bucket name (uses default if not provided)
            content_type: MIME type of the file
            metadata: Custom metadata to attach to the object

        Returns:
            bool: True if upload successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be provided either during initialization or method call")

        async def _upload_operation() -> bool:
            async with self.session.client('s3', endpoint_url=self.endpoint_url) as s3:
                async with aiofiles.open(local_file_path, 'rb') as file:
                    file_content = await file.read()

                    put_args = {
                        'Bucket': bucket,
                        'Key': s3_key,
                        'Body': file_content,
                        'ContentType': content_type
                    }

                    if metadata:
                        put_args['Metadata'] = metadata

                    await s3.put_object(**put_args)

                logging.info(f"Successfully uploaded {local_file_path} to s3://{bucket}/{s3_key}")
                return True

        return await self._retry_operation(
            _upload_operation,
            f"upload_file_object({local_file_path} -> s3://{bucket}/{s3_key})"
        )

    async def download_file(
            self,
            s3_key: str,
            local_file_path: str,
            bucket_name: Optional[str] = None
    ) -> bool:
        """
        Download a file from S3 with automatic retry on failure

        Args:
            s3_key: S3 key (path) of the file to download
            local_file_path: Local path where the file will be saved
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            bool: True if download successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be provided either during initialization or method call")

        async def _download_operation() -> bool:
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            async with self.session.client('s3', endpoint_url=self.endpoint_url) as s3:
                await s3.download_file(bucket, s3_key, local_file_path)
                logging.info(f"Successfully downloaded s3://{bucket}/{s3_key} to {local_file_path}")
                return True

        return await self._retry_operation(
            _download_operation,
            f"download_file(s3://{bucket}/{s3_key} -> {local_file_path})"
        )

    async def download_file_object(
            self,
            s3_key: str,
            local_file_path: str,
            bucket_name: Optional[str] = None
    ) -> bool:
        """
        Download a file using get_object for more control with automatic retry on failure

        Args:
            s3_key: S3 key (path) of the file to download
            local_file_path: Local path where the file will be saved
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            bool: True if download successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be provided either during initialization or method call")

        async def _download_operation() -> bool:
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

            async with self.session.client('s3', endpoint_url=self.endpoint_url) as s3:
                response = await s3.get_object(Bucket=bucket, Key=s3_key)

                async with aiofiles.open(local_file_path, 'wb') as file:
                    async for chunk in response['Body'].iter_chunks():
                        await file.write(chunk)

                logging.info(f"Successfully downloaded s3://{bucket}/{s3_key} to {local_file_path}")
                return True

        return await self._retry_operation(
            _download_operation,
            f"download_file_object(s3://{bucket}/{s3_key} -> {local_file_path})"
        )

    async def list_objects(
            self,
            prefix: str = "",
            bucket_name: Optional[str] = None,
            max_keys: int = 1000
    ) -> list:
        """
        List objects in S3 bucket

        Args:
            prefix: Filter objects by prefix
            bucket_name: S3 bucket name (uses default if not provided)
            max_keys: Maximum number of objects to return

        Returns:
            list: List of object information dictionaries
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be provided either during initialization or method call")

        try:
            async with self.session.client('s3', endpoint_url=self.endpoint_url) as s3:
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

                return objects

        except Exception as e:
            logging.error(f"Error listing objects: {str(e)}")
            return []

    async def delete_object(
            self,
            s3_key: str,
            bucket_name: Optional[str] = None
    ) -> bool:
        """
        Delete an object from S3

        Args:
            s3_key: S3 key (path) of the file to delete
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            bool: True if deletion successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be provided either during initialization or method call")

        try:
            async with self.session.client('s3', endpoint_url=self.endpoint_url) as s3:
                await s3.delete_object(Bucket=bucket, Key=s3_key)
                logging.info(f"Successfully deleted s3://{bucket}/{s3_key}")
                return True

        except Exception as e:
            logging.error(f"Error deleting object: {str(e)}")
            return False

    async def object_exists(
            self,
            s3_key: str,
            bucket_name: Optional[str] = None
    ) -> bool:
        """
        Check if an object exists in S3

        Args:
            s3_key: S3 key (path) of the file to check
            bucket_name: S3 bucket name (uses default if not provided)

        Returns:
            bool: True if object exists, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        if not bucket:
            raise ValueError("Bucket name must be provided either during initialization or method call")

        try:
            async with self.session.client('s3', endpoint_url=self.endpoint_url) as s3:
                await s3.head_object(Bucket=bucket, Key=s3_key)
                return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey' or error_code == '404':
                return False
            else:
                logging.error(f"Error checking object existence: {e}")
                return False
        except Exception as e:
            logging.error(f"Error checking object existence: {str(e)}")
            return False