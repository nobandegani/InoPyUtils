from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional, Union

@dataclass
class InoPhotoMetadata:
    def __init__(self, profile: Optional[str] = None):
        if profile == "iphone":
            self.iphone_profile()
        elif profile == "samsung":
            self.samsung_profile()

    camera_maker: Optional[str] = None
    camera_model: Optional[str] = None
    f_stop: Optional[Union[str, int, float]] = None            # EXIF FNumber
    exposure_time: Optional[Union[str, int, float]] = None     # EXIF ExposureTime
    iso_speed: Optional[Union[int, str]] = None                # EXIF ISOSpeedRatings (int)
    exposure_bias: Optional[Union[str, int, float]] = None     # EXIF ExposureBiasValue
    focal_length: Optional[Union[str, int, float]] = None      # EXIF FocalLength
    max_aperture: Optional[Union[str, int, float]] = None      # EXIF MaxApertureValue
    metering_mode: Optional[Union[int, str]] = None            # EXIF MeteringMode (enum int)
    subject_distance: Optional[Union[str, int, float]] = None  # EXIF SubjectDistance
    flash_mode: Optional[Union[int, str]] = None               # EXIF Flash (bitmask/int)
    flash_energy: Optional[Union[str, int, float]] = None      # EXIF FlashEnergy
    focal_length_35mm: Optional[Union[int, str]] = None        # EXIF FocalLengthIn35mmFilm

    lens_maker: Optional[str] = None
    lens_model: Optional[str] = None
    flash_maker: Optional[str] = None  # Not standard; will be placed in UserComment
    flash_model: Optional[str] = None  # Not standard; will be placed in UserComment
    camera_serial_number: Optional[str] = None                 # EXIF BodySerialNumber
    contrast: Optional[Union[int, str]] = None                 # EXIF Contrast (enum int)
    brightness: Optional[Union[str, int, float]] = None        # EXIF BrightnessValue
    light_source: Optional[Union[int, str]] = None             # EXIF LightSource (enum int)
    exposure_program: Optional[Union[int, str]] = None         # EXIF ExposureProgram (enum int)
    saturation: Optional[Union[int, str]] = None               # EXIF Saturation (enum int)
    sharpness: Optional[Union[int, str]] = None                # EXIF Sharpness (enum int)
    white_balance: Optional[Union[int, str]] = None            # EXIF WhiteBalance (enum int)
    photometric_interpretation: Optional[Union[int, str]] = None  # EXIF PhotometricInterpretation
    digital_zoom: Optional[Union[str, int, float]] = None      # EXIF DigitalZoomRatio
    exif_version: Optional[Union[str, bytes, int]] = None      # EXIF ExifVersion e.g., b"0231"

    # GPS
    gps_latitude: Optional[Union[str, float, int]] = None      # decimal degrees (N+=positive, S-=negative)
    gps_longitude: Optional[Union[str, float, int]] = None     # decimal degrees (E+=positive, W-=negative)
    gps_altitude: Optional[Union[str, float, int]] = None      # meters above sea level (negative = below)

    def iphone_profile(self):
        # Camera
        if self.camera_maker is None:
            self.camera_maker = "Apple"
        if self.camera_model is None:
            self.camera_model = "iPhone 13 Pro Max"

        # Core exposure
        if self.f_stop is None:
            self.f_stop = "f/1.5"  # FNumber
        if self.exposure_time is None:
            self.exposure_time = "1/60"  # ExposureTime
        if self.iso_speed in (None, "", 0):
            self.iso_speed = 80  # ISOSpeedRatings
        if self.exposure_bias is None:
            self.exposure_bias = 0  # ExposureBiasValue
        if self.focal_length is None:
            self.focal_length = "6 mm"  # FocalLength
        if self.max_aperture is None:
            self.max_aperture = "f/1.5"  # MaxApertureValue
        if self.metering_mode is None:
            self.metering_mode = "Pattern"  # MeteringMode
        if self.subject_distance is None:
            self.subject_distance = ""  # iPhone usually omits this
        if self.flash_mode is None:
            self.flash_mode = "No flash, compulsory"  # Flash
        if self.flash_energy is None:
            self.flash_energy = ""  # Not written by Apple
        if self.focal_length_35mm is None:
            self.focal_length_35mm = 26  # FocalLengthIn35mmFilm

        # Lens
        if self.lens_maker is None:
            self.lens_maker = "Apple"
        if self.lens_model is None:
            self.lens_model = "iPhone 13 Pro Max back triple camera"

        # Advanced / processing
        if self.camera_serial_number is None:
            self.camera_serial_number = ""  # Apple does not expose
        if self.contrast is None:
            self.contrast = ""  # Apple omits
        if self.brightness is None:
            self.brightness = 3.24989876493217267  # BrightnessValue
        if self.light_source is None:
            self.light_source = ""  # Usually undefined
        if self.exposure_program is None:
            self.exposure_program = "Normal"  # ExposureProgram
        if self.saturation is None:
            self.saturation = ""  # Apple omits
        if self.sharpness is None:
            self.sharpness = ""  # Apple omits
        if self.white_balance is None:
            self.white_balance = "Auto"  # WhiteBalance
        if self.photometric_interpretation is None:
            self.photometric_interpretation = "RGB"  # PhotometricInterpretation
        if self.digital_zoom is None:
            self.digital_zoom = "1.0"  # DigitalZoomRatio
        if self.exif_version is None:
            self.exif_version = b"0232"  # ExifVersion