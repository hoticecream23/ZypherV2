'''
"""
Zypher PDF Extractor - High Speed Asset Streamer
Focuses on fast extraction of Images and Raw Text.
Relies on MetadataCompressor for complex layout.
"""

from pydoc import doc
import fitz  # PyMuPDF
from typing import Dict, Generator, Any
from ..utils.logger import logger

class PDFExtractor:
    def __init__(self):
        self.supported_extensions = ['.pdf']

    

    def extract_streaming(self, pdf_path: str) -> Generator[Dict[str, Any], None, None]:
        """
        Yields PDF content page-by-page to keep memory footprint low.
        """
        doc = None
        try:
            doc = fitz.open(pdf_path)
            logger.info(f"Streaming extraction: {pdf_path} ({len(doc)} pages)")
            
            # 1. Yield Metadata First
            yield {
                'type': 'metadata',
                'metadata': self._extract_metadata(doc)
            }

            yield {
                'type': 'font_map',
                'fonts': self._extract_fonts(doc)
            }

            # 2. Yield Pages
            for page_index, page in enumerate(doc):
                page_num = page_index + 1
                
                # Extract Images (Raw Bytes)
                images = self._extract_images(page)
                
                # Extract Raw Text (For searchability/fallback)
                # Note: We don't need formatting here, MetadataCompressor handles that.
                text = page.get_text("text")

                yield {
                    'type': 'page',
                    'page_num': page_num,
                    'width': page.rect.width,
                    'height': page.rect.height,
                    'text': text,
                    'images': images
                }

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
        finally:
            if doc: doc.close()

    def _extract_page_content(self, page: fitz.Page) -> list:
        """
        Extracts granular text blocks using PyMuPDF.
        Fixes encoding mismatches by pulling span-level metadata.
        """
        blocks_data = []
        # Flags preserve ligatures and whitespace to prevent text scrambling
        page_dict = page.get_text("dict", flags=fitz.TEXT_PRESERVE_LIGATURES | fitz.TEXT_PRESERVE_WHITESPACE)
        
        for block in page_dict.get("blocks", []):
            if block["type"] == 0:  # Text type block
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        text = span["text"]
                        
                        # Filter out non-printable control characters that cause 'boxes'
                        clean_text = "".join(c for c in text if c.isprintable())
                        
                        if clean_text.strip():
                            blocks_data.append({
                                'text': clean_text,
                                'x': span["bbox"][0],
                                'y': span["bbox"][1],
                                'font': span["font"],
                                'size': span["size"],
                                'color': span["color"]
                            })
    

    def _extract_images(self, page: fitz.Page) -> list:
        """
        Extracts raw image data and bounding boxes.
        """
        image_list = []
        try:
            # get_images returns (xref, smask, width, height, bpc, colorspace, ...)
            raw_img_list = page.get_images(full=True)
            
            for i, img_info in enumerate(raw_img_list):
                xref = img_info[0]
                
                try:
                    # Extract the raw bytes
                    base_image = page.parent.extract_image(xref)
                    image_bytes = base_image["image"]
                    
                    # Get location on page (Bounding Box)
                    # This allows us to place it back exactly where it was
                    rects = page.get_image_rects(xref)
                    
                    # An image might be used multiple times on a page (e.g. a logo)
                    for bbox in rects:
                        image_list.append({
                            'data': image_bytes,
                            'ext': base_image['ext'],
                            'bbox': (bbox.x0, bbox.y0, bbox.x1, bbox.y1),
                            'xref': xref
                        })
                        
                except Exception as e:
                    logger.warning(f"Failed to extract image {xref}: {e}")
                    
        except Exception as e:
            logger.error(f"Image extraction error on page {page.number}: {e}")
            
        return image_list

__all__ = ["PDFExtractor"]
'''

"""
Zypher PDF Extractor - Unified High-Fidelity Engine
Uses PyMuPDF (fitz) for streaming extraction of Text, Images, Vectors, and Fonts.
"""
'''
import fitz  # PyMuPDF
from typing import List, Dict, Any, Generator
from ..utils.logger import logger

class PDFExtractor:
    def __init__(self):
        # Configuration for image extraction quality
        self.image_min_width = 10
        self.image_min_height = 10

    def extract_streaming(self, pdf_path: str) -> Generator[Dict[str, Any], None, None]:
        """
        Main entry point: Streams all document assets page by page.
        """
        doc = None
        try:
            doc = fitz.open(pdf_path)
            logger.info(f"Streaming extraction: {pdf_path} ({len(doc)} pages)")
            
            # 1. Yield Metadata
            yield {
                'type': 'metadata',
                'metadata': self._extract_metadata(doc)
            }

            # 2. Yield Global Fonts (Critical for perfect reconstruction)
            yield {
                'type': 'font_map',
                'fonts': self._extract_fonts(doc)
            }

            # 3. Iterate Pages for Content
            for page_index, page in enumerate(doc):
                page_num = page_index + 1
                
                # Extract components using unified fitz engine
                blocks = self._extract_page_content(page)
                images = self._extract_images(page)
                vectors = self._extract_vectors(page)

                yield {
                    'type': 'page',
                    'page_num': page_num,
                    'width': page.rect.width,
                    'height': page.rect.height,
                    'blocks': blocks,
                    'images': images,
                    'vectors': vectors
                }

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
        finally:
            if doc:
                doc.close()

    def _extract_metadata(self, doc: fitz.Document) -> Dict:
        """Extracts standard PDF metadata."""
        return doc.metadata

    def _extract_fonts(self, doc: fitz.Document) -> Dict[str, str]:
        """
        Extracts embedded font binaries and returns them as a Hex-encoded map.
        """
        font_map = {}
        for page in doc:
            for font in page.get_fonts(full=True):
                xref, basefont = font[0], font[3]
                if basefont in font_map:
                    continue
                try:
                    # Extract raw font binary
                    font_data = doc.extract_font(xref)[-1]
                    if font_data:
                        # Convert to Hex for JSON-safe transit via MetadataCompressor
                        font_map[basefont] = font_data.hex()
                except Exception:
                    continue
        return font_map

    def _extract_page_content(self, page: fitz.Page) -> List[Dict]:
        """
        Extracts granular text blocks with span-level metadata.
        Fixes gibberish by preserving ligatures and whitespace.
        """
        blocks_data = []
        # 'dict' format handles academic paper encodings much better than 'text'
        page_dict = page.get_text("dict", flags=fitz.TEXT_PRESERVE_LIGATURES | fitz.TEXT_PRESERVE_WHITESPACE)
        
        for block in page_dict.get("blocks", []):
            if block["type"] == 0:  # Text block
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        text = span["text"]
                        
                        # Filter out non-printable control characters that cause 'boxes'
                        clean_text = "".join(c for c in text if c.isprintable())
                        
                        if clean_text.strip():
                            blocks_data.append({
                                'text': clean_text,
                                'x': span["bbox"][0],
                                'y': span["bbox"][1],
                                'font': span["font"],
                                'size': span["size"],
                                'color': span["color"]
                            })
        return blocks_data

    def _extract_images(self, page: fitz.Page) -> List[Dict]:
        """Extracts image bytes and their positions."""
        images = []
        for img_index, img in enumerate(page.get_images(full=True)):
            xref = img[0]
            base_image = page.parent.extract_image(xref)
            image_bytes = base_image["image"]
            
            # Find image location on the page
            for img_info in page.get_image_info():
                if img_info['xref'] == xref:
                    images.append({
                        'data': image_bytes,
                        'bbox': list(img_info['bbox']),
                        'ext': base_image["ext"]
                    })
                    break
        return images

    def _extract_vectors(self, page: fitz.Page) -> List[Dict]:
        """Extracts vector drawings (lines, rectangles, curves)."""
        try:
            # get_drawings returns a list of dictionaries with paths/properties
            return page.get_drawings()
        except Exception:
            return []

__all__ = ["PDFExtractor"]
'''
"""
Zypher PDF Extractor - High Speed Asset Streamer
Focuses on fast extraction of Images and Raw Text.
Relies on MetadataCompressor for complex layout.
"""

import fitz  # PyMuPDF
from typing import Dict, Generator, Any, List
from ..utils.logger import logger


class PDFExtractor:
    def __init__(self):
        self.supported_extensions = ['.pdf']

    def extract_streaming(self, pdf_path: str) -> Generator[Dict[str, Any], None, None]:
        """
        Yields PDF content page-by-page to keep memory footprint low.
        """
        doc = None
        try:
            doc = fitz.open(pdf_path)
            logger.info(f"Streaming extraction: {pdf_path} ({len(doc)} pages)")
            
            # 1. Yield Metadata First
            yield {
                'type': 'metadata',
                'metadata': self._extract_metadata(doc)
            }

            # 2. Yield Font Map
            yield {
                'type': 'font_map',
                'fonts': self._extract_fonts(doc)
            }

            # 3. Yield Pages
            for page_index, page in enumerate(doc):
                page_num = page_index + 1
                
                # Always render full page as image for raster fallback
                mat = fitz.Matrix(1.5, 1.5)  # 1.5x is plenty for readability
                pix = page.get_pixmap(matrix=mat)
                full_page_image = {
                    'data': pix.tobytes("jpeg", jpg_quality=85),
                    'format': 'jpeg',
                    'bbox': (0, 0, page.rect.width, page.rect.height),
                    'page_num': page_num,
                    'is_full_page': True
                }
                
                # Also extract embedded images
                embedded_images = self._extract_images(page, page_num)
                
                text = page.get_text("text")

                yield {
                    'type': 'page',
                    'page_num': page_num,
                    'width': page.rect.width,
                    'height': page.rect.height,
                    'text': text,
                    'images': [full_page_image] + embedded_images
                }

        except Exception as e:
            logger.error(f"Extraction failed: {e}", exc_info=True)
            raise
        finally:
            if doc:
                doc.close()

    def _extract_metadata(self, doc: fitz.Document) -> Dict:
        """
        Extract PDF metadata
        
        Args:
            doc: PyMuPDF document
        
        Returns:
            Dictionary of metadata
        """
        metadata = doc.metadata or {}
        
        return {
            'title': metadata.get('title', ''),
            'author': metadata.get('author', ''),
            'subject': metadata.get('subject', ''),
            'keywords': metadata.get('keywords', ''),
            'creator': metadata.get('creator', ''),
            'producer': metadata.get('producer', ''),
            'creation_date': metadata.get('creationDate', ''),
            'modification_date': metadata.get('modDate', ''),
        }
    '''
    def _extract_fonts(self, doc: fitz.Document) -> Dict:
        """
        Extract embedded fonts from the PDF.
        Returns a dict mapping font names to their binary data (as hex strings).
        """
        font_map = {}
        
        try:
            for page_num in range(len(doc)):
                page = doc[page_num]
                
                # Get list of fonts used on this page
                font_list = page.get_fonts(full=True)
                
                for font_info in font_list:
                    xref = font_info[0]  # Font xref number
                    font_name = font_info[3]  # Font name
                    
                    # Skip if already extracted
                    if font_name in font_map:
                        continue
                    
                    try:
                        # Extract font binary data
                        font_buffer = doc.xref_stream(xref)
                        
                        if font_buffer:
                            # Store as hex string for JSON serialization
                            font_map[font_name] = font_buffer.hex()
                    except Exception as e:
                        logger.debug(f"Could not extract font {font_name}: {e}")
            
            if font_map:
                logger.info(f"Extracted {len(font_map)} embedded fonts")
        
        except Exception as e:
            logger.warning(f"Font extraction error: {e}")
        
        return font_map
        '''
    
    def _extract_fonts(self, doc: fitz.Document) -> Dict:
        font_map = {}
        try:
            for page_num in range(len(doc)):
                page = doc[page_num]
                font_list = page.get_fonts(full=True)

                for font_info in font_list:
                    xref = font_info[0]
                    font_name = font_info[3]   # basefont name

                    if font_name in font_map:
                        continue
                    try:
                        # extract_font returns (name, ext, type, encoding, referencer, content)
                        # index [-1] is the actual binary font data
                        font_tuple = doc.extract_font(xref)
                        font_binary = font_tuple[-1]

                        if font_binary:
                            font_map[font_name] = font_binary.hex()
                            # Also store under subset-stripped name (ABCDEF+FontName -> FontName)
                            if '+' in font_name:
                                clean = font_name.split('+')[1]
                                if clean not in font_map:
                                    font_map[clean] = font_map[font_name]
                    except Exception as e:
                        logger.debug(f"Could not extract font {font_name}: {e}")

            if font_map:
                logger.info(f"Extracted {len(font_map)} embedded fonts")

        except Exception as e:
            logger.warning(f"Font extraction error: {e}")

        return font_map

    def _extract_page_content(self, page: fitz.Page) -> List[Dict]:
        """
        Extracts granular text blocks using PyMuPDF.
        Fixes encoding mismatches by pulling span-level metadata.
        """
        blocks_data = []
        
        # Flags preserve ligatures and whitespace to prevent text scrambling
        page_dict = page.get_text(
            "dict", 
            flags=fitz.TEXT_PRESERVE_LIGATURES | fitz.TEXT_PRESERVE_WHITESPACE
        )
        
        for block in page_dict.get("blocks", []):
            if block["type"] == 0:  # Text type block
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        text = span["text"]
                        
                        # Filter out non-printable control characters that cause 'boxes'
                        clean_text = "".join(c for c in text if c.isprintable())
                        
                        if clean_text.strip():
                            blocks_data.append({
                                'text': clean_text,
                                'x': span["bbox"][0],
                                'y': span["bbox"][1],
                                'font': span["font"],
                                'size': span["size"],
                                'color': span["color"],
                                'flags': span.get("flags", 0)
                            })
        
        return blocks_data

    def _extract_images(self, page: fitz.Page, page_num: int) -> List[Dict]:
        """
        Extracts raw image data and bounding boxes.
        """
        image_list = []
        
        try:
            # get_images returns (xref, smask, width, height, bpc, colorspace, ...)
            raw_img_list = page.get_images(full=True)
            
            for i, img_info in enumerate(raw_img_list):
                xref = img_info[0]
                
                try:
                    # Extract the raw bytes
                    base_image = page.parent.extract_image(xref)
                    image_bytes = base_image["image"]
                    
                    # Get location on page (Bounding Box)
                    # This allows us to place it back exactly where it was
                    rects = page.get_image_rects(xref)
                    
                    # An image might be used multiple times on a page (e.g. a logo)
                    for bbox in rects:
                        image_list.append({
                            'data': image_bytes,
                            'format': base_image['ext'],
                            'bbox': (bbox.x0, bbox.y0, bbox.x1, bbox.y1),
                            'xref': xref,
                            'page_num': page_num
                        })
                        
                except Exception as e:
                    logger.debug(f"Failed to extract image xref {xref}: {e}")
                    
        except Exception as e:
            logger.warning(f"Image extraction error on page {page.number}: {e}")
            
        return image_list


__all__ = ["PDFExtractor"]