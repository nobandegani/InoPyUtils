from enum import Enum
from inocloudreve import CloudreveClient

from .file_helper import InoFileHelper


class SparkWorkflows(Enum):
    CAPTION_GENERATOR       = (0, "CaptionGenerator")
    DATASET_IMAGE_GENERATOR = (1, "DatasetImageGenerator")
    FACE_GENERATOR          = (2, "FaceGenerator")
    FACE_TOOL               = (3, "FaceTool")
    IMAGE_GENERATOR         = (4, "ImageGenerator")
    VIDEO_GENERATOR         = (5, "VideoGenerator")
    FACE_SWAPPER            = (6, "FaceSwapper")

    @property
    def id(self) -> int:
        return self.value[0]

    @property
    def label(self) -> str:
        return self.value[1]

class SparkHelper:
    @staticmethod
    async def get_batch_folder(cloud_client: CloudreveClient, workflow: SparkWorkflows, creator_name: str) -> dict:
        uri = f"Spark/Creators/{creator_name}/{workflow.FACE_SWAPPER.value}"

        last_folder_res = await cloud_client.get_last_folder_or_file(
            uri=uri
        )
        if not last_folder_res["status_code"] == 200 :
            return last_folder_res

        empty_folder = last_folder_res["last"] is None
        if empty_folder:
            batch_uri = uri + "/Batch_00001"
        else:
            last_name = last_folder_res["last_name"]
            increased_name = InoFileHelper.increment_batch_name(last_name)
            batch_uri = uri + "/" + increased_name
        return {
            "success": True,
            "msg": "getting batch folder successful",
            "uri": batch_uri
        }

    @staticmethod
    def get_default_storage_policy() -> dict:
        return {
            "success": True,
            "msg": "",
            "id": "O8cN",
            "name": "SparkDrive-2",
            "type": "s3",
            "max_size": 0
        }