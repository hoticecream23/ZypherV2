"""
Zypher Configuration
Single source of truth for all settings.
Load once at startup, pass to all components.
"""
import json
from pathlib import Path
from .utils.logger import logger


# Default config values
DEFAULTS = {
    "compression": {
        "default_level": "high",
        "max_file_size_mb": 500,
        "max_retries": 3,
        "retry_delay": 1.0,
        "chunk_size_kb": 64
    },
    "batch": {
        "max_workers": None,  # None = auto detect
        "show_per_file_progress": False
    },
    "dictionary": {
        "path": None,  # None = core/packager/zypher.dict
        "max_training_file_size_kb": 100,
        "dict_size_kb": 100
    },
    "extraction": {
        "ocr_enabled": True,
        "encoding_detection": True
    },
    "storage": {
        "input_dir": "input",
        "output_dir": "output",
        "restored_dir": "restored"
    }
}


class ZypherConfig:
    def __init__(self, config_path: str = None):
        self._config = self._deep_copy(DEFAULTS)

        if config_path:
            self.config_path = Path(config_path)
        else:
            # Look for config in project root
            self.config_path = Path(__file__).parent.parent / 'zypher.config.json'

        if self.config_path.exists():
            self._load()
        else:
            logger.info(f"No config file found at {self.config_path} — using defaults")

    def _load(self):
        """Load and merge config file over defaults"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                user_config = json.load(f)
            self._deep_merge(self._config, user_config)
            logger.info(f"Loaded config from {self.config_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {e} — using defaults")
        except Exception as e:
            logger.error(f"Failed to load config: {e} — using defaults")

    def save(self, path: str = None):
        """Save current config to file"""
        out_path = Path(path) if path else self.config_path
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(self._config, f, indent=2)
        logger.info(f"Config saved to {out_path}")

    def get(self, *keys, default=None):
        """
        Get a nested config value by key path.
        e.g. config.get('compression', 'default_level')
        """
        value = self._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def set(self, *keys_and_value):
        """
        Set a nested config value.
        e.g. config.set('compression', 'default_level', 'ultra')
        """
        *keys, value = keys_and_value
        target = self._config
        for key in keys[:-1]:
            target = target.setdefault(key, {})
        target[keys[-1]] = value

    # ── Convenience properties ─────────────────────────────────────────────

    @property
    def default_level(self) -> str:
        return self.get('compression', 'default_level', default='high')

    @property
    def max_file_size_mb(self) -> int:
        return self.get('compression', 'max_file_size_mb', default=500)

    @property
    def max_retries(self) -> int:
        return self.get('compression', 'max_retries', default=3)

    @property
    def retry_delay(self) -> float:
        return self.get('compression', 'retry_delay', default=1.0)

    @property
    def chunk_size(self) -> int:
        return self.get('compression', 'chunk_size_kb', default=64) * 1024

    @property
    def max_workers(self):
        return self.get('batch', 'max_workers', default=None)

    @property
    def dict_path(self):
        path = self.get('dictionary', 'path', default=None)
        if path:
            return Path(path)
        return Path(__file__).parent / 'packager' / 'zypher.dict'

    @property
    def dict_size_kb(self) -> int:
        return self.get('dictionary', 'dict_size_kb', default=100)

    @property
    def max_training_file_size_kb(self) -> int:
        return self.get('dictionary', 'max_training_file_size_kb', default=100)

    @property
    def ocr_enabled(self) -> bool:
        return self.get('extraction', 'ocr_enabled', default=True)

    @property
    def input_dir(self) -> str:
        return self.get('storage', 'input_dir', default='input')

    @property
    def output_dir(self) -> str:
        return self.get('storage', 'output_dir', default='output')

    @property
    def restored_dir(self) -> str:
        return self.get('storage', 'restored_dir', default='restored')

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _deep_copy(d: dict) -> dict:
        import copy
        return copy.deepcopy(d)

    @staticmethod
    def _deep_merge(base: dict, override: dict):
        """Merge override into base recursively — modifies base in place"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                ZypherConfig._deep_merge(base[key], value)
            else:
                base[key] = value


# Singleton — import this everywhere
config = ZypherConfig()

__all__ = ["ZypherConfig", "config"]