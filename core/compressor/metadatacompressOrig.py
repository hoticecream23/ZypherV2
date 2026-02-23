
'''
"""
Zypher Layout Engine & Metadata Compressor
Extracts layout coordinates using pdfplumber for perfect reconstruction.
"""
import json
import zstandard as zstd
from typing import Dict, Union, Any
from ..utils.logger import logger

# Conditional import to prevent crash if dependency is missing
try:
    import pdfplumber
    HAS_PLUMBER = True
except ImportError:
    HAS_PLUMBER = False

class MetadataCompressor:
    def __init__(self):
        # Layout maps are critical, we use max compression
        self.compressor = zstd.ZstdCompressor(level=22)
        self.decompressor = zstd.ZstdDecompressor()

    def extract_layout(self, file_path: str) -> Dict:
        """
        Extracts precise X,Y, Font, and Size data for every character.
        This is the 'Golden Data' required for the Rebuilder.
        """
        if not HAS_PLUMBER: 
            logger.warning("pdfplumber missing. Layout extraction skipped.")
            return {}
        
        layout = {}
        try:
            with pdfplumber.open(file_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    # We extract 'fontname' and 'size' specifically
                    words = page.extract_words(
                        extra_attrs=['fontname', 'size'],
                        x_tolerance=2, 
                        y_tolerance=2
                    )
                    
                    # Store as a compact list of blocks
                    layout[str(i+1)] = {
                        'width': float(page.width),
                        'height': float(page.height),
                        'blocks': [{
                            'text': w['text'],
                            'x': round(w['x0'], 2),
                            'y': round(w['top'], 2),
                            'font': w.get('fontname', 'Helvetica'),
                            'size': float(w.get('size', 10))
                        } for w in words]
                    }
            return layout
        except Exception as e:
            logger.error(f"Layout extraction error: {e}")
            return {}
    
V1
    def compress(self, data: Union[Dict, list]) -> bytes:
        # Use compact separators (no spaces) to save bytes before compression
        json_str = json.dumps(data, separators=(',', ':'))
        return self.compressor.compress(json_str.encode('utf-8'))

    def decompress(self, data: bytes) -> Dict:
        json_str = self.decompressor.decompress(data).decode('utf-8')
        return json.loads(json_str)
V1
    def compress(self, data: Any) -> bytes:
        """
        Serializes data to UTF-8 JSON before Zstd compression.
        ensure_ascii=False prevents corruption of math and special symbols.
        """
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        return self.compressor.compress(json_str.encode('utf-8'))

    def decompress(self, data: bytes) -> Any:
        """
        Decompresses and safely loads JSON back to Python objects using UTF-8.
        """
        decompressed_bytes = self.decompressor.decompress(data)
        return json.loads(decompressed_bytes.decode('utf-8'))
__all__ = ["MetadataCompressor"]

"""
Zypher Layout Engine & Metadata Compressor - FIXED
Extracts layout coordinates using pdfplumber + text from PyMuPDF for correct encoding.

FIX: Academic PDFs have encoding issues where pdfplumber extracts garbled text
     but PyMuPDF extracts it correctly. This merges both: coordinates from 
     pdfplumber (accurate), text from PyMuPDF (correct encoding via ToUnicode CMap)
"""     def extract_layout(self, file_path: str) -> Dict:
        """
        FIX: Groups words into lines to prevent overlap.
        Hybrid: pdfplumber coords + PyMuPDF text for correct encoding.
        """
        if not HAS_PLUMBER: 
            logger.warning("pdfplumber missing. Layout extraction skipped.")
            return {}
        
        layout = {}
        
        try:
            with pdfplumber.open(file_path) as plumber_pdf:
                fitz_doc = fitz.open(file_path) if HAS_FITZ else None
                
                for i, plumber_page in enumerate(plumber_pdf.pages):
                    page_num = i + 1
                    
                    # Step 1: Get word coordinates from pdfplumber
                    words = plumber_page.extract_words(
                        extra_attrs=['fontname', 'size'],
                        x_tolerance=15,  # ← Higher = less aggressive merging
                        y_tolerance=3
                    )
                    
                    # Step 2: Get correct text from PyMuPDF
                    if fitz_doc:
                        fitz_page = fitz_doc[i]
                        fitz_blocks = self._extract_fitz_text_blocks(fitz_page)
                        corrected_words = self._merge_coordinates_with_text(words, fitz_blocks)
                    else:
                        corrected_words = words
                    
                    # Step 3: FIX - Group words into lines by Y-coordinate
                    #lines = self._group_words_into_lines(corrected_words)
                    # Step 3: FIX - DON'T group, use words as-is
                    blocks = []
                    for word in corrected_words:
                        blocks.append({
                            'text': word['text'],
                            'x': round(word['x0'], 2),
                            'y': round(word['top'], 2),
                            'font': word.get('fontname', 'Helvetica'),
                            'size': float(word.get('size', 10)),
                            'flags': word.get('flags', 0)
                        })
                    
                    # Step 4: Convert lines to blocks format for rebuilder
                    blocks = []
                    for line in lines:
                        if line['words']:
                            # Combine all words in the line into one text string
                            text = ' '.join(w['text'] for w in line['words'])
                            first_word = line['words'][0]
                            
                            blocks.append({
                                'text': text,
                                'x': round(line['x0'], 2),
                                'y': round(line['y'], 2),
                                'font': first_word.get('fontname', 'Helvetica'),
                                'size': float(first_word.get('size', 10)),
                                'flags': first_word.get('flags', 0)
                            })
                    
                    layout[str(page_num)] = {
                        'width': float(plumber_page.width),
                        'height': float(plumber_page.height),
                        'blocks': blocks
                    }
                
                if fitz_doc:
                    fitz_doc.close()
            
            logger.info(f"Layout extracted: {len(layout)} pages, text grouped into lines")
            return layout
            
        except Exception as e:
            logger.error(f"Layout extraction error: {e}", exc_info=True)
            return {}
        


        def extract_layout(self, file_path: str) -> Dict:
        if not HAS_PLUMBER: 
            logger.warning("pdfplumber missing. Layout extraction skipped.")
            return {}
        
        layout = {}
        
        try:
            with pdfplumber.open(file_path) as plumber_pdf:
                fitz_doc = fitz.open(file_path) if HAS_FITZ else None
                
                for i, plumber_page in enumerate(plumber_pdf.pages):
                    page_num = i + 1
                    
                    words = plumber_page.extract_words(
                        extra_attrs=['fontname', 'size'],
                        x_tolerance=15,
                        y_tolerance=3
                    )
                    
                    if fitz_doc:
                        fitz_page = fitz_doc[i]
                        fitz_blocks = self._extract_fitz_text_blocks(fitz_page)
                        corrected_words = self._merge_coordinates_with_text(words, fitz_blocks)
                    else:
                        corrected_words = words
                    
                    blocks = []
                    for word in corrected_words:
                        blocks.append({
                            'text': word['text'],
                            'x': round(word['x0'], 2),
                            'y': round(word['top'], 2),
                            'font': word.get('fontname', 'Helvetica'),
                            'size': float(word.get('size', 10)),
                            'flags': word.get('flags', 0)
                        })
                    
                    layout[str(page_num)] = {
                        'width': float(plumber_page.width),
                        'height': float(plumber_page.height),
                        'blocks': blocks
                    }
                
                if fitz_doc:
                    fitz_doc.close()
            
            logger.info(f"Layout extracted: {len(layout)} pages, {sum(len(p['blocks']) for p in layout.values())} blocks")
            return layout
            
        except Exception as e:
            logger.error(f"Layout extraction error: {e}", exc_info=True)
            return {}
        
        """
            # Case 1: TeX Computer Modern fonts
            if any(font_name.startswith(prefix) for prefix in tex_fonts):
                logger.warning(f"Detected TeX font without ToUnicode: {font_name}")
                return False

            # Case 2: Type1 font without subset prefix (likely missing encoding)
            # Subset fonts usually look like "ABCDEE+FontName"
            if font_type == "Type1" and '+' not in font_info[3]:
                logger.warning(f"Detected unsubset Type1 font: {font_info[3]}")
                return False

        # If none of the problematic fonts found
        return True
    
        """
'''