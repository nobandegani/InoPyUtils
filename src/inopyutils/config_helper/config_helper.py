import configparser
from pathlib import Path

class InoConfigHelper:
    def __init__(self, path='configs/base.ini', save_as: bool = False, save_as_path: Path = None):
        self.debug = False
        self.path = Path(path)
        self.config = configparser.ConfigParser()
        self.save_as = save_as
        self.save_as_path = save_as_path

        self._load()

    def _load(self):
        self.config.read(self.path)

    def get(self, section, key, fallback=None):
        try:
            value = self.config.get(section, key, fallback=fallback).strip()
            if isinstance(value, list):
                print(f"❌ Config value for [{section}][{key}] is a list: {value}")
                return fallback
            if self.debug:
                print(f"🔎 Raw value for [{section}][{key}] = {value} ({type(value)})")
            return value
        except Exception as e:
            print(f"❌ Failed to get str for [{section}][{key}]: {e}")
            return fallback

    def get_bool(self, section, key, fallback=False):
        try:
            value=self.config.getboolean(section, key, fallback=fallback)
            if self.debug:
                print(f"🔎 Raw value for [{section}][{key}] = {value} ({type(value)})")
            return value
        except Exception as e:
            print(f"❌ Failed to get boolean for [{section}][{key}]: {e}")
            return fallback

    def set(self, section, key, value):
        if section not in self.config:
            self.config[section] = {}

        if self.debug or True:
            print(f"📝 Setting [{section}][{key}] = {value} ({type(value)})")

        self.config[section][key] = str(value).strip()

        self.save()

        self._load()

    def _is_valid_config(self):
        try:
            self.config.read(self.path)
            return bool(self.config.sections())
        except Exception:
            return False

    def save(self):
        if self.save_as:
            final_path = self.save_as_path
        else:
            final_path = self.path
        with open(final_path, "w") as configfile:
            self.config.write(configfile)