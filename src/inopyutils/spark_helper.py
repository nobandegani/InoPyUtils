from enum import Enum
from typing import Any, Optional
import re
from inocloudreve import CloudreveClient

from .file_helper import InoFileHelper


class SparkWorkflows(Enum):
    FACE_SWAPPER            = (0, "FaceSwapper")
    FACE_GENERATOR          = (1, "FaceGen")
    DATASET_IMAGE_GENERATOR = (2, "DatasetImageGen")
    FACE_TOOL               = (3, "FaceTool")
    CAPTION_GENERATOR       = (4, "CaptionGen")
    LORA_TRAINER            = (5, "LoraTrainer")
    IMAGE_GENERATOR         = (6, "ImageGen")
    VIDEO_GENERATOR         = (7, "VideoGen")
    AUDIO_GENERATOR         = (8, "AudioGen")

    @property
    def id(self) -> int:
        return self.value[0]

    @property
    def label(self) -> str:
        return self.value[1]

    @classmethod
    def _norm(cls, s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", s.lower())

    @classmethod
    def try_parse(cls, x: Any) -> Optional["SparkWorkflows"]:
        """Return enum if parsed, else None. Accepts id (int/str), label, or name."""
        # already enum
        if isinstance(x, cls):
            return x

        # numeric id (int or numeric string)
        if isinstance(x, int) or (isinstance(x, str) and x.strip().lstrip("-").isdigit()):
            code = int(x)
            for wf in cls:
                if wf.id == code:
                    return wf
            return None

        # match by label or enum name (case/format insensitive)
        if isinstance(x, str):
            norm_in = cls._norm(x)
            for wf in cls:
                if cls._norm(wf.label) == norm_in or cls._norm(wf.name) == norm_in:
                    return wf

        return None

    @classmethod
    def parse(cls, x: Any) -> "SparkWorkflows":
        """Strict parse: returns enum or raises ValueError."""
        wf = cls.try_parse(x)
        return wf

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