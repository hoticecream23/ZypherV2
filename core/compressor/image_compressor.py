"""
Zypher Image Compressor - Smart Segmenting Engine
Optimizes images based on content:
- B&W Scans -> CCITT Group 4 (TIFF)
- Photos/Color -> JPEG 2000 (JP2)
"""

import io
from PIL import Image
from ..utils.logger import logger

class ImageCompressor:
    def __init__(self, compression_level: str = 'high'):
        # Map generic levels to format-specific quality
        # 40dB is roughly visually lossless for JP2
        self.jp2_quality_layers = [80] if compression_level == 'high' else [40]

    def compress(self, image_data: bytes) -> bytes:
        """
        Smartly compresses image data based on visual content.
        Returns the compressed bytes in the optimal format (TIFF-G4 or JP2).
        """
        try:
            # We open the bytes without fully decoding to pixels immediately if possible,
            # but for analysis, we need the pixel data.
            with Image.open(io.BytesIO(image_data)) as img:
                
                # 1. Analyze: Is this a Black & White Scan?
                if self._is_monochrome_scan(img):
                    return self._compress_ccitt_g4(img)
                
                # 2. Fallback: It is a photo/color image -> JPEG 2000
                return self._compress_jpeg2000(img)

        except Exception as e:
            logger.warning(f"Smart compression failed: {e}. Storing original.")
            return image_data

    def _is_monochrome_scan(self, img: Image.Image) -> bool:
        """
        Detects if an image is effectively black and white text.
        Returns True if the image is suitable for 1-bit compression.
        """
        # If it's already 1-bit, obvious yes
        if img.mode == '1':
            return True
            
        # Convert to grayscale to check histogram
        # This is a fast operation
        gray = img.convert("L")
        histogram = gray.histogram()
        
        # Heuristic: Scanned text usually has pixels clustered at 0 (black) and 255 (white)
        # We check if > 90% of pixels exist at the very edges of the spectrum.
        total_pixels = img.width * img.height
        if total_pixels == 0: return False
        
        # Sum pixels at the dark end (0-10) and bright end (245-255)
        black_white_pixels = sum(histogram[:15]) + sum(histogram[-15:])
        
        return (black_white_pixels / total_pixels) > 0.90

    def _compress_ccitt_g4(self, img: Image.Image) -> bytes:
        """Compresses as 1-bit TIFF using CCITT Group 4 (Lossless, Tiny for Text)"""
        # Convert to 1-bit bitmap (dithering off for text clarity)
        bw = img.convert("1", dither=Image.NONE)
        
        out = io.BytesIO()
        # 'group4' is the specific compression algorithm used by Fax machines
        bw.save(out, format="TIFF", compression="group4")
        return out.getvalue()

    def _compress_jpeg2000(self, img: Image.Image) -> bytes:
        """Compresses as JPEG 2000 (Superior efficiency for color)"""
        out = io.BytesIO()
        try:
            # JPEG 2000 handles RGBA (Transparency), standard JPEG does not.
            # This is a huge advantage for preserving PDF layout fidelity.
            if img.mode == 'P':
                img = img.convert('RGBA')
            
            # Save as JP2
            img.save(out, format="JPEG2000", quality_mode='dB', quality_layers=self.jp2_quality_layers)
            return out.getvalue()
        except Exception:
            # Fallback to Optimized standard JPEG if system lacks JP2 drivers
            out.seek(0)
            out.truncate()
            # Convert to RGB because standard JPEG doesn't support Alpha
            img.convert('RGB').save(out, format="JPEG", optimize=True, quality=85)
            return out.getvalue()

__all__ = ["ImageCompressor"]