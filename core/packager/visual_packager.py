"""
Zypher Visual Packager
Visual fidelity mode — not byte-perfect.
Normalizes PDF structure + recompresses images in-place.
Best for: scanned docs, business documents, general archival.
Do NOT use for: signed PDFs, medical imaging, legal originals.
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
from ..config import config

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


class VisualPackager:
    MAGIC = b'ZPKV'  # distinct magic — V for Visual
    VERSION = 1
    CHUNK_SIZE = 65536

    SUPPORTED_FORMATS = {'.pdf'}  # visual mode PDF only for now

    COMPRESSION_LEVELS = {
        'low':    3,
        'medium': 10,
        'high':   19,
        'ultra':  22
    }

    def __init__(
        self,
        dict_path: str = None,
        max_file_size_mb: int = None,
        jpeg_quality: int = 85
    ):
        if not HAS_FITZ or not HAS_PIL:
            raise ImportError("pymupdf and Pillow required — pip install pymupdf pillow")

        self.dict_path = Path(dict_path) if dict_path else config.dict_path
        self.MAX_FILE_SIZE = (max_file_size_mb or config.max_file_size_mb) * 1024 * 1024
        self.jpeg_quality = jpeg_quality
        self._cached_dict = None
        self._load_dictionary()

    def _load_dictionary(self):
        if self.dict_path.exists():
            try:
                with open(self.dict_path, 'rb') as f:
                    self._cached_dict = zstd.ZstdCompressionDict(f.read())
                logger.info("Loaded zstd dictionary")
            except Exception as e:
                logger.warning(f"Failed to load dictionary: {e}")

    def compress_file(
        self,
        input_path: str,
        output_path: str,
        compression_level: str = 'high',
        on_progress=None
    ) -> dict:
        start_time = time.time()
        tmp_path = None
        tmp_pdf_path = None

        try:
            ext = Path(input_path).suffix.lower()
            if ext not in self.SUPPORTED_FORMATS:
                raise ValueError(f"VisualPackager only supports PDF, got: {ext}")

            original_size = os.path.getsize(input_path)
            if original_size == 0:
                raise ValueError(f"File is empty: {input_path}")
            if original_size > self.MAX_FILE_SIZE:
                raise ValueError(
                    f"File too large: {original_size/1024/1024:.1f}MB "
                    f"exceeds limit of {self.MAX_FILE_SIZE/1024/1024:.0f}MB"
                )

            logger.info(f"📦 Visual packaging: {input_path} [{compression_level}]")

            # Step 1: Open and inspect PDF
            doc = fitz.open(input_path)

            def _strip_metadata(self, doc: fitz.Document):
                """
                Strip all metadata from PDF — author, creator, 
                edit history, thumbnails, XMP data.
                No visual impact, free size savings.
                """
                try:
                    # Clear standard metadata fields
                    doc.set_metadata({
                        'title': '',
                        'author': '',
                        'subject': '',
                        'keywords': '',
                        'creator': '',
                        'producer': '',
                        'creationDate': '',
                        'modDate': ''
                    })

                    # Strip XMP metadata (extended metadata block)
                    doc.del_xml_metadata()

                    logger.info("   Metadata stripped")

                except Exception as e:
                    logger.debug(f"Metadata stripping failed: {e}")

            # Step 2: Skip signed PDFs — modifying them breaks the signature
            if self._is_signed(doc):
                doc.close()
                logger.warning(f"Signed PDF detected — falling back to lossless mode")
                from .packager import Packager
                return Packager(
                    dict_path=str(self.dict_path),
                    max_file_size_mb=self.MAX_FILE_SIZE // 1024 // 1024
                ).compress_file(input_path, output_path, compression_level, on_progress)

            # Step 3: Recompress images in-place
            before_images = self._total_image_size(doc)
            self._recompress_images(doc)
            after_images = self._total_image_size(doc)
            logger.info(
                f"   Images: {before_images/1024:.1f}KB → {after_images/1024:.1f}KB "
                f"({(1 - after_images/before_images)*100:.1f}% saved)"
                if before_images > 0 else "   No images found"
            )

            # Step 4: Normalize and strip internal compression
            processed_bytes = doc.tobytes(
                garbage=3,
                clean=True,
                deflate=False,  # strip — zstd handles compression
            )
            doc.close()

            logger.info(
                f"   Normalized: {original_size/1024:.1f}KB → "
                f"{len(processed_bytes)/1024:.1f}KB"
            )

            # Write processed PDF to temp file for streaming
            tmp_pdf_fd, tmp_pdf_path = tempfile.mkstemp(suffix='.pdf')
            with os.fdopen(tmp_pdf_fd, 'wb') as f:
                f.write(processed_bytes)
            processed_size = len(processed_bytes)

            # Step 5: zstd compress the processed PDF
            level = self.COMPRESSION_LEVELS.get(compression_level, 19)
            use_ldm = processed_size > 1_000_000
            params = zstd.ZstdCompressionParameters.from_level(
                level,
                enable_ldm=1 if use_ldm else 0,
                ldm_hash_log=20 if use_ldm else 0,
                threads=0 if on_progress else -1
            )
            cctx = zstd.ZstdCompressor(
                compression_params=params,
                dict_data=self._cached_dict if self._cached_dict else None
            )

            # Checksum of processed bytes — not original
            # (we store what we can restore, not the original bytes)
            checksum = hashlib.sha256(processed_bytes).hexdigest()

            manifest = {
                'original_filename': Path(input_path).name,
                'original_size': original_size,
                'processed_size': processed_size,
                'compression_level': compression_level,
                'format': 'pdf',
                'mode': 'visual',
                'jpeg_quality': self.jpeg_quality,
                'checksum': checksum,
                'has_dict': self._cached_dict is not None
            }
            manifest_bytes = json.dumps(manifest).encode('utf-8')

            # Atomic write
            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix='.zpkv.tmp',
                dir=Path(output_path).parent
            )

            chunk_size = self._get_chunk_size(processed_size)

            with os.fdopen(tmp_fd, 'wb') as out_f:
                out_f.write(self.MAGIC)
                out_f.write(struct.pack('>BL', self.VERSION, len(manifest_bytes)))
                out_f.write(manifest_bytes)

                with cctx.stream_writer(out_f, closefd=False) as compressor:
                    with open(tmp_pdf_path, 'rb') as in_f:
                        bytes_read = 0
                        while chunk := in_f.read(chunk_size):
                            compressor.write(chunk)
                            bytes_read += len(chunk)
                            if on_progress:
                                on_progress(bytes_read, processed_size)

            shutil.move(tmp_path, output_path)
            tmp_path = None

            final_size = os.path.getsize(output_path)
            elapsed = time.time() - start_time
            percent = (1 - final_size / original_size) * 100

            logger.info(f"✨ Visual Compression Complete!")
            logger.info(f"   Original:  {original_size/1024:.2f} KB")
            logger.info(f"   Zypher:    {final_size/1024:.2f} KB")
            logger.info(f"   Saved:     {percent:.1f}%")
            logger.info(f"   Time:      {elapsed:.2f}s")

            return {
                'success': True,
                'mode': 'visual',
                'output_file': output_path,
                'original_size': original_size,
                'compressed_size': final_size,
                'compression_ratio': final_size / original_size,
                'space_saved_percent': percent,
                'processing_time': elapsed
            }

        except Exception as e:
            logger.error(f"Visual packaging failed: {e}", exc_info=True)
            raise
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            if tmp_pdf_path and os.path.exists(tmp_pdf_path):
                os.remove(tmp_pdf_path)

    def _is_signed(self, doc: fitz.Document) -> bool:
        """Detect digital signatures — modifying signed PDFs breaks them"""
        try:
            for page in doc:
                for widget in (page.widgets() or []):
                    if widget.field_type_string == 'Signature':
                        return True
            return '/ByteRange' in str(doc.pdf_trailer())
        except Exception:
            return False

    def _total_image_size(self, doc: fitz.Document) -> int:
        """Sum of all image byte sizes in document"""
        total = 0
        seen = set()
        for page in doc:
            for img in page.get_images(full=True):
                xref = img[0]
                if xref in seen:
                    continue
                seen.add(xref)
                try:
                    total += len(doc.extract_image(xref)['image'])
                except Exception:
                    pass
        return total

    def _recompress_images(self, doc: fitz.Document):
        seen = set()
        replaced = 0
        skipped = 0

        for page in doc:
            imgs = page.get_images(full=True)
            logger.info(f"   Page {page.number + 1}: {len(imgs)} images found")  # ADD THIS
            
            for img in imgs:
                xref = img[0]
                width, height = img[2], img[3]
                logger.info(f"   xref={xref} size={width}x{height}")  # ADD THIS
                
                if xref in seen:
                    continue
                seen.add(xref)

                if width < 100 or height < 100:
                    skipped += 1
                    continue

                try:
                    base = doc.extract_image(xref)
                    with Image.open(io.BytesIO(base['image'])) as pil_img:
                        out = io.BytesIO()
                        if pil_img.mode in ('RGBA', 'P', 'L'):
                            pil_img = pil_img.convert('RGB')
                        pil_img.save(
                            out,
                            format='JPEG',
                            quality=self.jpeg_quality,
                            optimize=True
                        )
                        recompressed = out.getvalue()

                    # Only replace if actually smaller
                    if len(recompressed) < len(base['image']):
                        doc.update_image(xref, stream=recompressed)
                        replaced += 1
                    else:
                        logger.info(
                            f"   xref={xref}: recompressed {len(base['image'])/1024:.1f}KB → "
                            f"{len(recompressed)/1024:.1f}KB — skipping (already optimized)"
                        )
                        skipped += 1

                except Exception as e:
                    logger.debug(f"Skipping image xref {xref}: {e}")
                    skipped += 1

        logger.info(f"   Images replaced: {replaced}, skipped: {skipped}")

    def _get_chunk_size(self, file_size: int) -> int:
        if file_size < 10_000_000:
            return 65536
        elif file_size < 100_000_000:
            return 524288
        else:
            return 1048576

    def _checksum_file(self, file_path: str) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(self.CHUNK_SIZE):
                sha256.update(chunk)
        return sha256.hexdigest()


__all__ = ["VisualPackager"]