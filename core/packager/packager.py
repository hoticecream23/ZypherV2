"""
Zypher Packager
Lossless compression using zstd streaming.
Dictionary cached in memory, LDM for large files.
"""
import os
import json
import time
import tempfile
import shutil
import struct
import hashlib
from pathlib import Path
import zstandard as zstd
from ..utils.logger import logger
from typing import Optional, Callable
from ..config import config

class Packager:
    SUPPORTED_FORMATS = {
        '.pdf', '.jpg', '.jpeg', '.png',
        '.tiff', '.docx', '.xlsx', '.pptx', '.txt', '.csv'
    }

    COMPRESSION_LEVELS = {
        'low':    3,
        'medium': 10,
        'high':   19,
        'ultra':  22
    }

    MAGIC = b'ZPKG'
    VERSION = 1
    CHUNK_SIZE = 65536          # 64KB read chunks
    MAX_TRAINING_FILE_SIZE = 102400  # 100KB max for dict training samples
    # At class level
    MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB default — adjust to your server limits

    def __init__(self, dict_path: str = None, max_file_size_mb: int = 500):
        # Fix #3: accept absolute path, don't rely on cwd
        if dict_path:
            self.dict_path = Path(dict_path)
        else:
            # Default to same directory as this file
            self.dict_path = Path(__file__).parent / 'zypher.dict'

        self.MAX_FILE_SIZE = max_file_size_mb * 1024 * 1024

        # Fix #1: load dictionary once into memory at init
        self._cached_dict = None
        self._cached_decompressor_dict = None
        self._load_dictionary()

    def _load_dictionary(self):
        """Load dictionary into memory once during initialization"""
        if self.dict_path.exists():
            try:
                with open(self.dict_path, 'rb') as f:
                    dict_data = f.read()
                self._cached_dict = zstd.ZstdCompressionDict(dict_data)
                self._cached_decompressor_dict = zstd.ZstdCompressionDict(dict_data)
                logger.info(f"Loaded zstd dictionary into memory ({len(dict_data)/1024:.1f} KB)")
            except Exception as e:
                logger.warning(f"Failed to load dictionary: {e}")

    def _get_chunk_size(self, file_size: int) -> int:
        if file_size < 10_000_000:      # < 10MB
            return 65536                 # 64KB
        elif file_size < 100_000_000:   # < 100MB
            return 524288                # 512KB
        else:                           # > 100MB
            return 1048576               # 1MB

    def compress_file(self, input_path: str, output_path: str, compression_level: str = 'high', on_progress: Optional[Callable] = None) -> dict:
        start_time = time.time()
        tmp_path = None

        try:
            logger.info(f"Packaging: {input_path} [{compression_level}]")

            ext = Path(input_path).suffix.lower()

            if ext not in self.SUPPORTED_FORMATS:
                raise ValueError(f"Unsupported file format: {ext}")

            original_size = os.path.getsize(input_path)
            if original_size == 0:
                raise ValueError(f"File is empty: {input_path}")
            
            if original_size > self.MAX_FILE_SIZE:
                raise ValueError(
                    f"File too large: {original_size/1024/1024:.1f}MB exceeds "
                    f"limit of {self.MAX_FILE_SIZE/1024/1024:.0f}MB"
                )

            checksum = self._checksum_file(input_path)
            level = self.COMPRESSION_LEVELS.get(compression_level, 19)
            cctx = self._build_compressor(level, original_size, track_progress=on_progress is not None)

            manifest = {
                'original_filename': Path(input_path).name,
                'original_size': original_size,
                'compression_level': compression_level,
                'format': ext.lstrip('.'),
                'checksum': checksum,
                'has_dict': self._cached_dict is not None
            }
            manifest_bytes = json.dumps(manifest).encode('utf-8')

            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix='.zpkg.tmp',
                dir=Path(output_path).parent
            )

            with os.fdopen(tmp_fd, 'wb') as out_f:
                out_f.write(self.MAGIC)
                out_f.write(struct.pack('>BL', self.VERSION, len(manifest_bytes)))
                out_f.write(manifest_bytes)

                #with cctx.stream_writer(out_f, closefd=False) as compressor:
                    #with open(input_path, 'rb') as in_f:
                        #while chunk := in_f.read(self.CHUNK_SIZE):
                            #compressor.write(chunk)

                # AFTER
                chunk_size = self._get_chunk_size(original_size)
                with cctx.stream_writer(out_f, closefd=False) as compressor:
                    with open(input_path, 'rb') as in_f:
                        bytes_read = 0
                        while chunk := in_f.read(chunk_size):
                            compressor.write(chunk)
                            bytes_read += len(chunk)
                            if on_progress:
                                on_progress(bytes_read, original_size)

            shutil.move(tmp_path, output_path)
            tmp_path = None

            final_size = os.path.getsize(output_path)
            elapsed = time.time() - start_time
            percent = (1 - final_size / original_size) * 100

            logger.info(f" Compression Complete!")
            logger.info(f"   Original:  {original_size/1024:.2f} KB")
            logger.info(f"   Zypher:    {final_size/1024:.2f} KB")
            logger.info(f"   Saved:     {percent:.1f}%")
            logger.info(f"   Time:      {elapsed:.2f}s")

            return {
                'success': True,
                'output_file': output_path,
                'original_size': original_size,
                'compressed_size': final_size,
                'compression_ratio': final_size / original_size,
                'space_saved_percent': percent,
                'processing_time': elapsed
            }

        except Exception as e:
            logger.error(f"Packaging failed: {e}", exc_info=True)
            raise
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _build_compressor(self, level: int, file_size: int, track_progress: bool = False) -> zstd.ZstdCompressor:
        """Build compressor with LDM for large files, cached dict if available"""
        use_ldm = file_size > 1_000_000

        params = zstd.ZstdCompressionParameters.from_level(
            level,
            enable_ldm=1 if use_ldm else 0,
            ldm_hash_log=20 if use_ldm else 0,
            threads=-1 if not track_progress else 0
        )

        if self._cached_dict:
            return zstd.ZstdCompressor(compression_params=params, dict_data=self._cached_dict)

        return zstd.ZstdCompressor(compression_params=params)


    def compress_with_retry(
        self,
        input_path: str,
        output_path: str,
        compression_level: str = 'high',
        on_progress: Optional[Callable] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> dict:
        """
        Compression with automatic retry on transient failures.
        Does not retry on permanent errors (unsupported format, file too large, empty file).
        """
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                return self.compress_file(
                    input_path,
                    output_path,
                    compression_level,
                    on_progress
                )

            except ValueError as e:
                # Permanent errors — don't retry
                # (unsupported format, file too large, file empty)
                logger.error(f"Permanent error, not retrying: {e}")
                raise

            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(
                        f"Compression failed (attempt {attempt}/{max_retries}): {e} "
                        f"— retrying in {retry_delay}s"
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # exponential backoff
                else:
                    logger.error(f"All {max_retries} attempts failed for {input_path}")

        raise last_error

    def train_dictionary(
        self,
        sample_files: list,
        dict_output_path: str = None,
        dict_size_kb: int = 100
    ) -> str:
        """
        Train a zstd dictionary on sample files.
        Only uses files under 100KB — larger files don't benefit from dictionaries
        and waste RAM during training.
        """
        if not sample_files:
            raise ValueError("No sample files provided for training")

        output_path = dict_output_path or str(self.dict_path)
        samples = []
        total_size = 0
        skipped = 0

        logger.info(f"Training dictionary on up to {len(sample_files)} files...")

        for file_path in sample_files:
            try:
                # Fix #2: skip large files — dict training only helps small files
                size = os.path.getsize(file_path)
                if size > self.MAX_TRAINING_FILE_SIZE:
                    logger.debug(f"  Skipping {file_path} (too large: {size/1024:.1f} KB)")
                    skipped += 1
                    continue

                with open(file_path, 'rb') as f:
                    data = f.read()
                samples.append(data)
                total_size += len(data)

            except Exception as e:
                logger.warning(f"  Skipping {file_path}: {e}")

        if not samples:
            raise ValueError(
                f"No eligible files for training — all {skipped} files exceeded "
                f"{self.MAX_TRAINING_FILE_SIZE/1024:.0f}KB limit. "
                f"Dictionary training only helps small files."
            )

        logger.info(
            f"Training on {len(samples)} files "
            f"({total_size/1024:.1f} KB total, {skipped} skipped as too large)..."
        )

        try:
            dictionary = zstd.train_dictionary(dict_size_kb * 1024, samples)
        except Exception as e:
            raise RuntimeError(f"Dictionary training failed: {e}")

        with open(output_path, 'wb') as f:
            f.write(dictionary.as_bytes())

        dict_size = os.path.getsize(output_path)
        logger.info(f"✅ Dictionary saved: {output_path} ({dict_size/1024:.1f} KB)")

        # Reload into memory cache immediately
        self.dict_path = Path(output_path)
        self._load_dictionary()

        return output_path

    def _checksum_file(self, file_path: str) -> str:
        """Compute SHA-256 without loading into RAM"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(self.CHUNK_SIZE):
                sha256.update(chunk)
        return sha256.hexdigest()


__all__ = ["Packager"]