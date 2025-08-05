from enum import Enum, auto
from dataclasses import dataclass

class SparkWorkflows(Enum):
    CAPTION_GENERATOR        = "caption_generator"
    DATASET_IMAGE_GENERATOR  = "dataset_image_generator"
    FACE_GENERATOR           = "face_generator"
    FACE_TOOL                = "face_tool"
    IMAGE_GENERATOR          = "image_generator"
    VIDEO_GENERATOR          = "video_generator"
    FACE_SWAPPER             = "face_swapper"


