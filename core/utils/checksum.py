"""
Zypher Checksum Utility
Calculates SHA-256 hashes for data integrity verification.
"""
import hashlib
from typing import Union

def calculate_bytes_checksum(data: Union[bytes, str]) -> str:
    """
    Calculates the SHA-256 checksum of a byte string or text string.
    
    Args:
        data: The input data (bytes or string)
        
    Returns:
        str: The hexadecimal hash string
    """
    if isinstance(data, str):
        data = data.encode('utf-8')
    
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data)
    return sha256_hash.hexdigest()

def calculate_file_checksum(file_path: str) -> str:
    """
    Calculates the SHA-256 checksum of a file efficiently.
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read in 64kb chunks to save memory
        for byte_block in iter(lambda: f.read(65536), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

__all__ = ["calculate_bytes_checksum", "calculate_file_checksum"]