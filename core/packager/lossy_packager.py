"""
Zypher Lossy Packager
Optional module for scanned PDF compression.
Recompresses embedded images at lower quality for better ratios.
NOT byte-perfect — use only when visual fidelity > exact restoration.
"""
import os
import io
import json
import time
import tempfile
import shutil
import struct
import hashlib
from pathlib import Path
import zstandard as zstd
from ..utils.logger import logger

try:
    import fitz
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False


class LossyPackager:
    """
    Use this instead of Packager when:
    - Documents are scanned PDFs (images wrapped in PDF)
    - Storage space is critical
    - Users don't need byte-perfect restoration
    - Visual quality is acceptable at slight degradation

    Do NOT use for:
    - Legal/medical/archival documents requiring exact reproduction
    - Documents where users may verify file integrity externally
    """

    MAGIC = b'ZPKL'  # different magic to distinguish from lossless .zpkg
    VERSION = 1
    CHUNK_SIZE = 65536
    JPEG_QUALITY = 85  # visually near-identical, meaningfully smaller

    def __init__(self, dict_path: str = None, jpeg_quality: int = 85):
        if dict_path:
            self.dict_path = Path(dict_path)
        else:
            self.dict_path = Path(__file__).parent / 'zypher.dict'

        self.jpeg_quality = jpeg_quality
        self._cached_dict = None
        self._load_dictionary()

    def _load_dictionary(self):
        if self.dict_path.exists():
            try:
                with open(self.dict_path, 'rb') as f:
                    self._cached_dict = zstd.ZstdCompressionDict(f.read())
                logger.info("Loaded zstd dictionary into memory")
            except Exception as e:
                logger.warning(f"Failed to load dictionary: {e}")

    def compress_file(self, input_path: str, output_path: str, compression_level: str = 'high') -> dict:
        if not HAS_FITZ or not HAS_PIL:
            raise ImportError("pymupdf and Pillow required for lossy compression")

        ext = Path(input_path).suffix.lower()
        if ext != '.pdf':
            raise ValueError("LossyPackager only supports PDF files")

        start_time = time.time()
        tmp_path = None
        tmp_pdf = None

        LEVELS = {'low': 3, 'medium': 10, 'high': 19, 'ultra': 22}
        level = LEVELS.get(compression_level, 19)

        try:
            logger.info(f"📦 Lossy packaging: {input_path} [{compression_level}]")

            original_size = os.path.getsize(input_path)

            # Step 1: Recompress images inside the PDF
            tmp_pdf_fd, tmp_pdf = tempfile.mkstemp(suffix='.pdf')
            os.close(tmp_pdf_fd)

            recompressed_size = self._recompress_pdf_images(input_path, tmp_pdf)
            logger.info(f"   PDF image recompression: {original_size/1024:.1f}KB → {recompressed_size/1024:.1f}KB")

            # Step 2: zstd compress the recompressed PDF
            manifest = {
                'original_filename': Path(input_path).name,
                'original_size': original_size,
                'recompressed_pdf_size': recompressed_size,
                'compression_level': compression_level,
                'format': 'pdf',
                'lossy': True,
                'jpeg_quality': self.jpeg_quality,
                'checksum': self._checksum_file(tmp_pdf),  # checksum of recompressed version
                'has_dict': self._cached_dict is not None
            }
            manifest_bytes = json.dumps(manifest).encode('utf-8')

            # Build compressor
            params = zstd.ZstdCompressionParameters.from_level(
                level,
                enable_ldm=1 if recompressed_size > 1_000_000 else 0,
                ldm_hash_log=20 if recompressed_size > 1_000_000 else 0,
            )
            cctx = zstd.ZstdCompressor(
                compression_params=params,
                dict_data=self._cached_dict if self._cached_dict else None
            )

            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix='.zpkl.tmp',
                dir=Path(output_path).parent
            )

            with os.fdopen(tmp_fd, 'wb') as out_f:
                out_f.write(self.MAGIC)
                out_f.write(struct.pack('>BL', self.VERSION, len(manifest_bytes)))
                out_f.write(manifest_bytes)

                with cctx.stream_writer(out_f, closefd=False) as compressor:
                    with open(tmp_pdf, 'rb') as in_f:
                        while chunk := in_f.read(self.CHUNK_SIZE):
                            compressor.write(chunk)

            shutil.move(tmp_path, output_path)
            tmp_path = None

            final_size = os.path.getsize(output_path)
            elapsed = time.time() - start_time
            percent = (1 - final_size / original_size) * 100

            logger.info(f"✨ Lossy Compression Complete!")
            logger.info(f"   Original:  {original_size/1024:.2f} KB")
            logger.info(f"   Zypher:    {final_size/1024:.2f} KB")
            logger.info(f"   Saved:     {percent:.1f}%")
            logger.info(f"   Time:      {elapsed:.2f}s")

            return {
                'success': True,
                'lossy': True,
                'output_file': output_path,
                'original_size': original_size,
                'compressed_size': final_size,
                'compression_ratio': final_size / original_size,
                'space_saved_percent': percent,
                'processing_time': elapsed
            }

        except Exception as e:
            logger.error(f"Lossy packaging failed: {e}", exc_info=True)
            raise
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            if tmp_pdf and os.path.exists(tmp_pdf):
                os.remove(tmp_pdf)

    def _recompress_pdf_images(self, input_path: str, output_path: str) -> int:
        """
        Opens PDF, finds all images, recompresses them at target JPEG quality,
        replaces them in the PDF, saves. Returns new file size.
        """
        doc = fitz.open(input_path)

        for page in doc:
            for img_info in page.get_images(full=True):
                xref = img_info[0]
                try:
                    base_image = doc.extract_image(xref)
                    image_bytes = base_image['image']
                    img_ext = base_image['ext'].lower()

                    # Only recompress JPEG and PNG — skip tiny images
                    width = img_info[2]
                    height = img_info[3]
                    if width < 100 or height < 100:
                        continue

                    with Image.open(io.BytesIO(image_bytes)) as img:
                        out = io.BytesIO()

                        if img.mode in ('RGBA', 'P'):
                            img = img.convert('RGB')

                        img.save(out, format='JPEG', quality=self.jpeg_quality, optimize=True)
                        recompressed = out.getvalue()

                    # Only replace if actually smaller
                    if len(recompressed) < len(image_bytes):
                        doc.update_image(xref, stream=recompressed)

                except Exception as e:
                    logger.debug(f"Skipping image xref {xref}: {e}")
                    continue

        doc.save(output_path, deflate=True, garbage=3)
        size = os.path.getsize(output_path)
        doc.close()
        return size

    def _checksum_file(self, file_path: str) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(self.CHUNK_SIZE):
                sha256.update(chunk)
        return sha256.hexdigest()


__all__ = ["LossyPackager"]