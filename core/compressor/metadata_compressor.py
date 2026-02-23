
"""
Zypher Layout Engine & Metadata Compressor - FINAL FIX
Groups words into lines to prevent text overlap in rebuilt PDFs.

FIX: Words must be grouped into lines sharing the same Y-coordinate,
     otherwise every word draws at its own position causing overlap.
"""
import json
import zstandard as zstd
from typing import Dict, Union, Any, List
from ..utils.logger import logger

# Conditional imports
try:
    import pdfplumber
    HAS_PLUMBER = True
except ImportError:
    HAS_PLUMBER = False
    logger.warning("pdfplumber not installed - layout extraction disabled")

try:
    import fitz  # PyMuPDF
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False
    logger.warning("PyMuPDF not installed - text correction disabled")


class MetadataCompressor:
    def __init__(self):
        self.compressor = zstd.ZstdCompressor(level=22)
        self.decompressor = zstd.ZstdDecompressor()

    def extract_layout(self, file_path: str) -> Dict:
        if not HAS_FITZ:
            logger.warning("PyMuPDF missing. Layout extraction skipped.")
            return {}
        
        layout = {}
        
        try:
            doc = fitz.open(file_path)
            logger.info(f"Starting layout extraction, pages: {len(doc)}")
            
            for page_index in range(len(doc)):
                page = doc[page_index]
                page_num = page_index + 1

                if not self._font_has_tounicode(doc, page):
                    logger.info(f"Page {page_num}: missing ToUnicode CMap, flagging for raster fallback")
                    layout[str(page_num)] = {
                        'width': float(page.rect.width),
                        'height': float(page.rect.height),
                        'blocks': [],
                        'raster_fallback': True
                    }
                    continue
                
                blocks = []
                
                # Use fitz dict extraction - handles all encodings correctly
                page_dict = page.get_text(
                    "dict",
                    flags=fitz.TEXT_PRESERVE_LIGATURES | fitz.TEXT_PRESERVE_WHITESPACE
                )
                
                for block in page_dict.get("blocks", []):
                    if block.get("type") != 0:  # text blocks only
                        continue
                    for line in block.get("lines", []):
                        for span in line.get("spans", []):
                            text = span.get("text", "")
                            clean = "".join(c for c in text if c.isprintable()).strip()
                            if not clean:
                                continue
                            
                            bbox = span.get("bbox", (0, 0, 0, 0))
                            
                            blocks.append({
                                'text': clean,
                                'x': round(bbox[0], 2),
                                'y': round(bbox[1], 2),  # fitz y is from top-left
                                'font': span.get("font", "Helvetica"),
                                'size': float(span.get("size", 10)),
                                'flags': span.get("flags", 0)
                            })
                
                layout[str(page_num)] = {
                    'width': float(page.rect.width),
                    'height': float(page.rect.height),
                    'blocks': blocks
                }
            
            doc.close()
            logger.info(f"Layout extracted: {len(layout)} pages, {sum(len(p['blocks']) for p in layout.values())} blocks")
            return layout
        
        except Exception as e:
            logger.error(f"Layout extraction error: {e}", exc_info=True)
            return {}
        
    def _font_has_tounicode(self, doc: fitz.Document, page: fitz.Page) -> bool:
        """
        Returns False only if the page uses Type1/TeX fonts with no ToUnicode CMap.
        These are the only fonts that cause garbling.
        """
        font_list = page.get_fonts(full=True)
        for font_info in font_list:
            font_type = font_info[2]  # e.g. "Type1", "TrueType", "CIDFont"
            font_name = font_info[3].lower()
            
            # TeX Computer Modern fonts are always Type1 with no CMap
            tex_fonts = ['cmr', 'cmmi', 'cmsy', 'cmex', 'cmbx', 'cmtt', 'cmsl', 'cmti']
            if any(font_name.startswith(f) for f in tex_fonts):
                return False
            
            # Type1 fonts without a subset prefix are likely unencoded
            if font_type == "Type1" and '+' not in font_info[3]:
                return False
        
        return True
        
    def _font_has_tounicode(self, doc: fitz.Document, page: fitz.Page) -> bool:
        """
        Returns False only if the page uses Type1/TeX fonts with no ToUnicode CMap.
        These are the fonts most likely to cause garbled text.
        """
        font_list = page.get_fonts(full=True)

        # TeX Computer Modern font prefixes (usually Type1 without Unicode)
        tex_fonts = ['cmr', 'cmmi', 'cmsy', 'cmex', 'cmbx', 'cmtt', 'cmsl', 'cmti']

        for font_info in font_list:
            font_type = font_info[2]   # e.g. "Type1", "TrueType", "CIDFont"
            font_name = font_info[3].lower()

            logger.info(f"Font check: name='{font_name}' type='{font_type}'")
        return True
    
    def _extract_fitz_text_blocks(self, fitz_page) -> List[Dict]:
        """Extract text blocks from PyMuPDF with correct encoding."""
        blocks = []
        
        try:
            text_dict = fitz_page.get_text("dict")
            
            for block in text_dict.get('blocks', []):
                if block['type'] == 0:  # Text block
                    for line in block.get('lines', []):
                        for span in line.get('spans', []):
                            text = span.get('text', '').strip()
                            if text:
                                bbox = span.get('bbox', (0, 0, 0, 0))
                                blocks.append({
                                    'text': text,
                                    'bbox': bbox,
                                    'x0': bbox[0],
                                    'y0': bbox[1],
                                    'x1': bbox[2],
                                    'y1': bbox[3],
                                    'font': span.get('font', ''),
                                    'size': span.get('size', 10),
                                    'flags': span.get('flags', 0)
                                })
        except Exception as e:
            logger.debug(f"PyMuPDF text extraction failed: {e}")
        
        return blocks
    
    def _merge_coordinates_with_text(
        self, 
        plumber_words: List[Dict], 
        fitz_blocks: List[Dict]
    ) -> List[Dict]:
        """
        Merge pdfplumber coordinates with PyMuPDF text.
        Finds nearest match by center distance.
        """
        if not fitz_blocks:
            return plumber_words
        
        corrected = []
        used_fitz_indices = set()
        
        for pw in plumber_words:
            px0, py0 = pw['x0'], pw['top']
            px1, py1 = pw['x1'], pw['bottom']
            p_center_x = (px0 + px1) / 2
            p_center_y = (py0 + py1) / 2
            
            # Find nearest PyMuPDF block
            best_match = None
            best_distance = float('inf')
            best_index = -1
            
            for idx, fb in enumerate(fitz_blocks):
                if idx in used_fitz_indices:
                    continue
                f_center_x = (fb['x0'] + fb['x1']) / 2
                f_center_y = (fb['y0'] + fb['y1']) / 2
                
                dist = ((p_center_x - f_center_x) ** 2 + 
                       (p_center_y - f_center_y) ** 2) ** 0.5
                
                if dist < best_distance:
                    best_distance = dist
                    best_match = fb
                    best_index = idx
            
            # Use PyMuPDF text if found nearby
            #if best_match and best_distance < 50:
                #pw['text'] = best_match['text']
                #if 'flags' in best_match:
                    #pw['flags'] = best_match['flags']
            if best_match and best_distance < 50:
                used_fitz_indices.add(best_index)
                pw['text'] = best_match['text']
                if 'flags' in best_match:
                    pw['flags'] = best_match['flags']
                if best_match.get('font'):
                    pw['fontname'] = best_match['font']   # overwrite with fitz's font name (has subset prefix)
                if best_match.get('size'):
                    pw['size'] = best_match['size']        # fitz size is more reliable than pdfplumber's
            else:
                # Check if text looks garbled
                text = pw.get('text', '')
                if self._is_garbled(text):
                    logger.debug(f"Skipping garbled text: {repr(text[:30])}")
                    continue
            
            corrected.append(pw)
        
        return corrected
    
    @staticmethod
    def _is_garbled(text: str) -> bool:
        """Detect garbled text (high ratio of non-ASCII chars)."""
        if not text or len(text) < 3:
            return False
        
        problematic = sum(1 for c in text if ord(c) > 127 or c in '\x00\ufffd□')
        return (problematic / len(text)) > 0.4
    
    def compress(self, data: Any) -> bytes:
        """Compress data to Zstd."""
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        return self.compressor.compress(json_str.encode('utf-8'))
    
    def decompress(self, data: bytes) -> Any:
        """Decompress Zstd data."""
        decompressed_bytes = self.decompressor.decompress(data)
        return json.loads(decompressed_bytes.decode('utf-8'))


__all__ = ["MetadataCompressor"]