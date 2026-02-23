"""
Zypher Unpacker
Streaming decompression with integrity verification.
Dictionary cached in memory at init.
"""
import os
import json
import time
import struct
import tempfile
import shutil
import hashlib
from pathlib import Path
import zstandard as zstd
from ..utils.logger import logger


class Unpacker:
    MAGIC = b'ZPKG'
    CHUNK_SIZE = 65536

    def __init__(self, dict_path: str = None):
        if dict_path:
            self.dict_path = Path(dict_path)
        else:
            self.dict_path = Path(__file__).parent / 'zypher.dict'

        self._cached_dict = None
        self._load_dictionary()

    def _load_dictionary(self):
        """Load dictionary into memory once during initialization"""
        if self.dict_path.exists():
            try:
                with open(self.dict_path, 'rb') as f:
                    self._cached_dict = zstd.ZstdCompressionDict(f.read())
                logger.info(f"Loaded zstd dictionary into memory")
            except Exception as e:
                logger.warning(f"Failed to load dictionary: {e}")

    def unpack(self, package_path: str, output_dir: str) -> dict:
        start_time = time.time()
        tmp_path = None

        try:
            logger.info(f"🔓 Unpacking: {package_path}")

            with open(package_path, 'rb') as f:
                magic = f.read(4)
                if magic not in {b'ZPKG', b'ZPKV'}:
                    raise ValueError(f"Invalid .zpkg file — bad magic bytes")

                version, manifest_len = struct.unpack('>BL', f.read(5))
                manifest = json.loads(f.read(manifest_len).decode('utf-8'))

                out_dir = Path(output_dir)
                out_dir.mkdir(parents=True, exist_ok=True)

                dctx = self._build_decompressor(manifest.get('has_dict', False))

                tmp_fd, tmp_path = tempfile.mkstemp(dir=out_dir, suffix='.tmp')

                with os.fdopen(tmp_fd, 'wb') as out_f:
                    with dctx.stream_reader(f) as reader:
                        while chunk := reader.read(self.CHUNK_SIZE):
                            out_f.write(chunk)

            # Verify checksum
            stored_checksum = manifest.get('checksum')
            if stored_checksum:
                actual_checksum = self._checksum_file(tmp_path)
                if actual_checksum != stored_checksum:
                    raise ValueError("Checksum mismatch — file corrupted or tampered with")
                logger.info(f"✅ Integrity verified")
            else:
                logger.warning("No checksum in manifest — skipping integrity check")

            # Re-apply internal PDF compression for visual archives
            if manifest.get('mode') == 'visual':
                self._recompress_pdf_streams(tmp_path)

            # Move to final path — only once
            final_path = out_dir / manifest['original_filename']
            shutil.move(tmp_path, final_path)
            tmp_path = None

            logger.info(f"✅ Restored: {final_path}")

            return {
                'success': True,
                'output_path': str(final_path),
                'original_size': manifest.get('original_size'),
                'time': time.time() - start_time
            }

        except ValueError as e:
            logger.error(f"Integrity check failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unpack failed: {e}", exc_info=True)
            raise
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _build_decompressor(self, has_dict: bool) -> zstd.ZstdDecompressor:
        """Use cached dictionary if file was compressed with one"""
        if has_dict and self._cached_dict:
            return zstd.ZstdDecompressor(dict_data=self._cached_dict)
        return zstd.ZstdDecompressor()
    
    def _recompress_pdf_streams(self, pdf_path: str):
        try:
            import fitz
            tmp_fd, tmp_recomp = tempfile.mkstemp(
                suffix='.pdf', 
                dir=Path(pdf_path).parent
            )
            
            doc = fitz.open(pdf_path)
            data = doc.tobytes(deflate=True)
            doc.close()
            
            with os.fdopen(tmp_fd, 'wb') as f:
                f.write(data)
            
            # Remove original then rename — Windows safe
            os.remove(pdf_path)
            os.rename(tmp_recomp, pdf_path)
            
            logger.info(f"   PDF streams recompressed")

        except Exception as e:
            logger.warning(f"Failed to recompress PDF streams: {e}")

    def _checksum_file(self, file_path: str) -> str:
        """Compute SHA-256 without loading into RAM"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(self.CHUNK_SIZE):
                sha256.update(chunk)
        return sha256.hexdigest()


__all__ = ["Unpacker"]