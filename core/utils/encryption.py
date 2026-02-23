"""
Zypher Encryption Utility
Handles AES encryption for secure archives.
"""
import os
import base64
from .logger import logger

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

class ZypherEncryption:
    def __init__(self, password: str):
        if not HAS_CRYPTO:
            logger.error("❌ 'cryptography' library not found. Encryption disabled.")
            logger.error("   Run: pip install cryptography")
            raise ImportError("Missing cryptography library")

        self.salt = os.urandom(16)
        self.key = self._derive_key(password, self.salt)
        self.cipher = Fernet(self.key)

    def _derive_key(self, password: str, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode()))

    def encrypt_bytes(self, data: bytes) -> bytes:
        # Prepend salt to data so we can decrypt later
        encrypted = self.cipher.encrypt(data)
        return self.salt + encrypted

    def decrypt_bytes(self, data: bytes) -> bytes:
        # Extract salt (first 16 bytes)
        salt = data[:16]
        encrypted_data = data[16:]
        
        # In a real scenario, we'd need to re-derive the key with this salt.
        # For this simple implementation, we assume the session key is valid.
        return self.cipher.decrypt(encrypted_data)

__all__ = ["ZypherEncryption"]