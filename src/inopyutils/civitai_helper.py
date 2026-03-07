import os
import asyncio

from pathlib import Path

from .util_helper import ino_is_err, ino_ok, ino_err
from .file_helper import InoFileHelper
from .http_helper import InoHttpHelper

class InoCivitHelper:
    def __init__(self, token: str | None = None):
        if token is None:
            token = os.getenv("CIVITAI_TOKEN", token)

        self.token = token

        self.default_headers = {
            "Authorization": f"Bearer {token}"
        }

        self.http_client = InoHttpHelper(
            timeout_total=None,
            timeout_sock_read=None,
            timeout_connect=30.0,
            timeout_sock_connect=30.0,
            retries=5,
            backoff_factor=1.0,
            default_headers=self.default_headers
        )

    async def close(self):
        await self.http_client.close()

    async def get_model(self, model_id: int):
        get_model_req = await self.http_client.get(
            url=f"https://civitai.com/api/v1/models/{model_id}"
        )
        return {
            "success": get_model_req["success"],
            "msg": get_model_req["msg"],
            "status_code": get_model_req["status_code"],
            "model": get_model_req["data"],
            "headers": get_model_req["headers"],
        }

    async def get_model_version(self, model_version: int):
        get_model_req = await self.http_client.get(
            url=f"https://civitai.com/api/v1/model-versions/{model_version}"
        )
        return {
            "success": get_model_req["success"],
            "msg": get_model_req["msg"],
            "status_code": get_model_req["status_code"],
            "model": get_model_req["data"],
            "files": get_model_req["data"]["files"],
            "headers": get_model_req["headers"],
        }

    async def verify_local_file(self, local_file_path: Path, remote_sha: str, chunk_size:int = 8):
        if local_file_path.is_file():
            local_sha_res = await InoFileHelper.get_file_hash_sha_256(local_file_path, chunk_size)

            if ino_is_err(local_sha_res):
                return local_sha_res

            local_sha:str = local_sha_res["sha"]

            if local_sha.lower() == remote_sha.lower():
                return ino_ok(f"Download model skipped, model already downloaded and verified", verified=True)
            else:
                local_file_path.unlink()
                return ino_ok("file not verified", verified=False)

        return ino_ok("file not found", verified=False)

    async def download_model(self, model_path: Path, model_id: int, model_version: int, file_id:int = 0, chunk_size:int = 8):
        get_model_req = await self.get_model_version(model_version)

        if ino_is_err(get_model_req):
            return get_model_req

        model = get_model_req["model"]

        remote_files = get_model_req["files"]

        try:
            remote_file = remote_files[file_id]
        except IndexError:
            return ino_err(f"file_id is not valid")


        remote_file_url = remote_file["downloadUrl"]
        remote_file_sha:str = remote_file["hashes"]["SHA256"]

        local_file_path: Path = model_path / remote_file["name"]

        verify_local_res = await self.verify_local_file(local_file_path, remote_file_sha, chunk_size)
        if ino_is_err(verify_local_res):
            return verify_local_res

        if verify_local_res["verified"]:
            return ino_ok(
                f"Download model skipped, model already downloaded and verified",
                model=model,
                remote_file=remote_file,
                remote_files=remote_files
            )
        else:
            print(verify_local_res["msg"])

        download_file_res = await self.http_client.download(
            url=remote_file_url,
            dest_path=model_path,
            chunk_size= chunk_size * 1024 * 1024,
            resume=True,
            overwrite=True,
            allow_redirects=True,
            mkdirs=True,
            verify_size=True,
        )
        if ino_is_err(download_file_res):
            return download_file_res

        if download_file_res["status_code"] != 200:
            return ino_err(f"download failed", status_code=download_file_res["status_code"], status_msg=download_file_res["msg"])

        verify_local_res = await self.verify_local_file(local_file_path, remote_file_sha, chunk_size)
        if ino_is_err(verify_local_res) or not verify_local_res["verified"]:
            return ino_err(f"download completed but file not verified")

        return ino_ok(
            f"Download model completed, and file verified",
            model=model,
            remote_file=remote_file,
            remote_files=remote_files
        )
