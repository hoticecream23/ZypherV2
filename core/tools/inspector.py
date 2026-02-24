"""
Zypher Archive Inspector
Peek inside a .zpkg archive without decompressing.
"""
import json
import struct
from pathlib import Path
from datetime import datetime
from ..utils.logger import logger


class Inspector:
    MAGIC_MAP = {
        b'ZPKG': 'lossless',
        b'ZPKV': 'visual'
    }

    def inspect(self, package_path: str) -> dict:
        """
        Read manifest from archive without decompressing content.
        """
        path = Path(package_path)
        if not path.exists():
            raise ValueError(f"Archive not found: {package_path}")

        archive_size = path.stat().st_size

        with open(package_path, 'rb') as f:
            # Read header
            magic = f.read(4)
            if magic not in self.MAGIC_MAP:
                raise ValueError(f"Not a valid Zypher archive")

            version, manifest_len = struct.unpack('>BL', f.read(5))
            manifest = json.loads(f.read(manifest_len).decode('utf-8'))

        mode = self.MAGIC_MAP[magic]
        original_size = manifest.get('original_size', 0)
        saving = (1 - archive_size / original_size) * 100 if original_size > 0 else 0
        ratio = original_size / archive_size if archive_size > 0 else 0

        info = {
            'archive_path': str(path),
            'archive_size': archive_size,
            'mode': mode,
            'version': version,
            'original_filename': manifest.get('original_filename'),
            'original_size': original_size,
            'format': manifest.get('format'),
            'compression_level': manifest.get('compression_level'),
            'checksum': manifest.get('checksum'),
            'has_dict': manifest.get('has_dict', False),
            'space_saved_percent': saving,
            'compression_ratio': ratio,
            'jpeg_quality': manifest.get('jpeg_quality'),
        }

        self._print(info)
        return info

    def _print(self, info: dict):
        def fmt_size(b):
            if b is None or b == 0:
                return 'unknown'
            if b >= 1024 * 1024:
                return f"{b/1024/1024:.2f} MB"
            return f"{b/1024:.1f} KB"

        print(f"\n{'='*50}")
        print(f"  Zypher Archive Inspection")
        print(f"{'='*50}")
        print(f"  File:        {info['archive_path']}")
        print(f"  Mode:        {info['mode']}")
        print(f"  Version:     {info['version']}")
        print(f"  Format:      {info['format'] or 'unknown'}")
        print(f"  Level:       {info['compression_level'] or 'unknown'}")
        print()
        print(f"  Original:    {fmt_size(info['original_size'])}")
        print(f"  Compressed:  {fmt_size(info['archive_size'])}")
        print(f"  Saved:       {info['space_saved_percent']:.1f}%")
        print(f"  Ratio:       {info['compression_ratio']:.2f}x")
        print()
        print(f"  Checksum:    {info['checksum'] or 'none'}")
        print(f"  Dictionary:  {'yes' if info['has_dict'] else 'no'}")
        if info['jpeg_quality']:
            print(f"  JPEG quality:{info['jpeg_quality']}")
        print(f"{'='*50}\n")


__all__ = ["Inspector"]