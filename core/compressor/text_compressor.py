"""
Zypher Text Compressor - Ultra Mode
Forces maximum compression for text streams.
"""
import zstandard as zstd
from typing import Union
from ..utils.logger import logger

class TextCompressor:
    def __init__(self, level: str = 'high'):
        # Force Ultra High Compression (Level 22)
        # Text is small enough that the extra CPU time is negligible
        self.level = 22 
        self.compressor = zstd.ZstdCompressor(level=self.level)
        self.decompressor = zstd.ZstdDecompressor()

    def compress(self, text: Union[str, bytes]) -> bytes:
        """Compress text string or bytes"""
        if isinstance(text, str):
            text = text.encode('utf-8')
        return self.compressor.compress(text)

    def decompress(self, data: bytes) -> str:
        """Decompress bytes back to string"""
        return self.decompressor.decompress(data).decode('utf-8')

__all__ = ["TextCompressor"]