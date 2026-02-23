"""
Zypher PDF Rebuilder - Pure PyMuPDF Engine
Complete implementation handling:
1. Native Font Injection (TTF/OTF/CFF)
2. Vector Graphics Reconstruction
3. High-Fidelity Text Layout
4. Native Image Injection
"""

import fitz  # PyMuPDF
import json
import zstandard as zstd
from typing import List, Dict, Any, Tuple
from ..utils.logger import logger

class PDFRebuilder:

    SAFE_MODE = False

    def rebuild(self, chunks: List[Dict], output_path: str, manifest: Dict) -> None:
        """
        Reconstructs the PDF from compressed chunks.
        """
        try:
            logger.info(f"🔨 Rebuilding: {output_path}")
            
            # 1. Initialize Empty PDF
            doc = fitz.open()
            
            # 2. Organize Data
            layout = manifest.get('page_layouts', {})
            if not layout:
                logger.warning("⚠️ No layout data found! Output will be empty.")
            
            # Map chunks by ID for fast lookup
            img_map = {c['id']: c for c in chunks if c['type'] == 'image'}
            vec_map = {c['id']: c for c in chunks if c['type'] == 'vectors'}
            
            # 3. Load Global Fonts
            # We load the binary data once to reuse across pages
            #font_buffers = self._load_fonts(chunks)

            # 3. Load Global Fonts
            font_buffers = {}
            # Only load custom fonts if we are NOT in Safe Mode
            if not self.SAFE_MODE:
                font_buffers = self._load_fonts(chunks)

            # 4. Build Pages Sequentially
            sorted_pages = sorted(layout.keys(), key=lambda k: int(k))
            
            for p_num in sorted_pages:
                p_data = layout[p_num]
                width = p_data['width']
                height = p_data['height']
                
                # Create the page
                page = doc.new_page(width=width, height=height)
                
                # A. Draw Vectors (Background Layer)
                # We draw these first so text appears on top of them
                v_chunk = next((v for v in vec_map.values() 
                               if str(v.get('metadata', {}).get('page_num')) == str(p_num)), None)
                if v_chunk:
                    self._draw_vectors(page, v_chunk)

                # B. Draw Text (Middle Layer)
                # We pass the font buffers to register them on-the-fly
                self._draw_text(page, p_data.get('blocks', []), font_buffers)

                # C. Draw Images (Top Layer/Overlay)
                self._draw_images(page, p_num, img_map)

            # 5. Save Final PDF
            # deflate=True ensures the newly created PDF structure is compact
            doc.save(output_path, deflate=True)
            doc.close()
            
            logger.info(f"✅ PDF Rebuilt Successfully: {output_path}")

        except Exception as e:
            logger.error(f"Rebuild failed: {e}", exc_info=True)
            raise

    def _load_fonts(self, chunks: List[Dict]) -> Dict[str, bytes]:
        font_buffers = {}
        font_chunk = next((c for c in chunks if c['type'] == 'fonts'), None)
        
        if font_chunk:
            try:
                dctx = zstd.ZstdDecompressor()
                # 1. Decompress bytes -> JSON String
                f_json_bytes = dctx.decompress(font_chunk['data'])
                
                # 2. Decode bytes -> String
                f_json_str = f_json_bytes.decode('utf-8')
                
                # 3. Parse String -> Dictionary (FIX IS HERE)
                # We need to ensure we are parsing the *inner* JSON content
                font_data_map = json.loads(f_json_str)
                
                # Safety check: if it's still a string (double-serialized), load it again
                if isinstance(font_data_map, str):
                    font_data_map = json.loads(font_data_map)

                for name, hex_data in font_data_map.items():
                    font_buffers[name] = bytes.fromhex(hex_data)
                    if '+' in name:
                        clean = name.split('+')[1]
                        font_buffers[clean] = font_buffers[name]
                
                logger.info(f"   🔤 Loaded {len(font_data_map)} embedded fonts")
            except Exception as e:
                logger.warning(f"Font load error: {e}")
        
        return font_buffers

    def _draw_vectors(self, page: fitz.Page, v_chunk: Dict):
        """
        Reconstructs vector graphics (lines, rects, curves) from JSON.
        """
        try:
            dctx = zstd.ZstdDecompressor()
            v_json = dctx.decompress(v_chunk['data'])
            vectors = json.loads(v_json)
            
            # Create a shape object to optimize drawing calls
            shape = page.new_shape()
            
            for obj in vectors:
                # 1. Apply Styles
                if 'color' in obj and obj['color']:
                    shape.finish() # Commit previous path
                    shape.draw_color = obj['color'] # Set stroke
                if 'fill' in obj and obj['fill']:
                    shape.finish()
                    shape.fill_color = obj['fill'] # Set fill
                
                # 2. Draw Paths
                for item in obj.get('items', []):
                    cmd = item[0]
                    
                    if cmd == "l": # Line: (p1, p2)
                        p1, p2 = item[1], item[2]
                        shape.draw_line(p1['x'], p1['y'], p2['x'], p2['y'])
                        
                    elif cmd == "re": # Rect: (rect)
                        r = item[1]
                        # PyMuPDF Rect format: x0, y0, x1, y1
                        rect = fitz.Rect(r['x0'], r['y0'], r['x1'], r['y1'])
                        shape.draw_rect(rect)
                        
                    elif cmd == "c": # Bezier Curve: (p1, p2, p3, p4)
                        p1, p2, p3, p4 = item[1], item[2], item[3], item[4]
                        shape.draw_bezier(
                            fitz.Point(p1['x'], p1['y']),
                            fitz.Point(p2['x'], p2['y']),
                            fitz.Point(p3['x'], p3['y']),
                            fitz.Point(p4['x'], p4['y'])
                        )
                
                # 3. Commit this shape
                shape.finish(
                    fill=obj.get('fill') is not None, 
                    stroke=obj.get('color') is not None
                )
            
            # Write all shapes to the page
            shape.commit()
            
        except Exception as e:
            logger.warning(f"Vector draw warning: {e}")
    '''
    def _draw_text(self, page: fitz.Page, blocks: List, font_buffers: Dict):
        """
        Draws text using exact coordinates and embedded fonts.
        """
        registered_on_page = set()
        
        for block in blocks:
            text = block['text'].strip()
            if not text: continue
            
            # --- Font Selection ---
            font_name = block['font']
            clean_name = font_name.split('+')[1] if '+' in font_name else font_name
            
            target_font = "helv" # Default fallback
            
            # Check if we have the binary for this font
            if font_name in font_buffers:
                target_font = font_name
            elif clean_name in font_buffers:
                target_font = clean_name
            
            # Register Font if needed (only once per page per font)
            # This is CRITICAL: We inject the binary data into the PDF here
            if target_font != "helv" and target_font not in registered_on_page:
                try:
                    page.insert_font(fontname=target_font, fontbuffer=font_buffers[target_font])
                    registered_on_page.add(target_font)
                except Exception:
                    # If font binary is corrupt, fallback to helv
                    target_font = "helv"

            # --- Text Insertion ---
            # pdfplumber coords are Top-Left. PyMuPDF text insertion needs Baseline.
            # We approximate baseline as y + size (since y is top)
            x = block['x']
            y_baseline = block['y'] + block['size']

            try:
                # insert_text automatically handles the font reference created above
                page.insert_text(
                    (x, y_baseline),
                    text,
                    fontname=target_font,
                    fontsize=block['size']
                )
            except Exception:
                # Fallback for encoding errors
                try:
                    page.insert_text(
                        (x, y_baseline),
                        text,
                        fontname="helv",
                        fontsize=block['size']
                    )
                except: pass
    '''
    def _draw_text(self, page: fitz.Page, blocks: List, font_buffers: Dict):
        """
        Smart Font Reconstruction:
        1. If the font is a "Standard" type (Times/Arial/etc), force system fonts.
           -> Fixes 'gibberish' in academic papers & saves space.
        2. If the font is "Unique" (brand fonts, handwriting), use the embedded binary.
           -> Preserves visual fidelity.
        """
        registered_on_page = set()
        
        # Standard fonts that are safer to map to system equivalents
        # These are the usual suspects for "broken encodings" in PDFs
        STANDARD_FONTS_TRIGGERS = [
            'times', 'roman', 'serif',  # Maps to Times-Roman
            'arial', 'helvetica', 'sans', 'robot', 'calibri', # Maps to Helvetica
            'courier', 'mono', 'typewriter' # Maps to Courier
        ]

        for block in blocks:
            text = block['text'].strip()
            if not text: continue
            
            orig_name = block['font'].lower()
            clean_name = orig_name.split('+')[1] if '+' in orig_name else orig_name
            
            font_face = None
            use_custom = True

            # --- STRATEGY 1: Is it a Standard Font? (Force System) ---
            # If the PDF says "TimesNewRomanPSMT", just use real Times-Roman.
            # It looks 99% identical but guarantees the text isn't gibberish.
            if any(trig in orig_name for trig in STANDARD_FONTS_TRIGGERS):
                use_custom = False
                
                if any(x in orig_name for x in ['courier', 'mono']):
                    font_face = "cour"
                elif any(x in orig_name for x in ['times', 'roman', 'serif']):
                    font_face = "ti" # Short for Times-Roman in PyMuPDF
                else:
                    font_face = "helv" # Helvetica (matches Arial/Calibri best)

            # --- STRATEGY 2: Is it a Unique/Custom Font? (Try Embedded) ---
            if use_custom:
                # Restore original casing for lookup
                real_name = block['font']
                real_clean = real_name.split('+')[1] if '+' in real_name else real_name
                
                target = None
                if real_name in font_buffers: target = real_name
                elif real_clean in font_buffers: target = real_clean
                
                if target:
                    font_face = target
                    # Register binary if needed
                    if target not in registered_on_page:
                        try:
                            page.insert_font(fontname=target, fontbuffer=font_buffers[target])
                            registered_on_page.add(target)
                        except:
                            # If the custom font binary is corrupt, fallback to Helvetica
                            font_face = "helv" 
                else:
                    # We don't have the binary, must fallback
                    font_face = "helv"

            # --- DRAW ---
            # PyMuPDF expects Baseline coordinates (approx y + size)
            x, y = block['x'], block['y'] + block['size']

            try:
                page.insert_text((x, y), text, fontname=font_face, fontsize=block['size'])
            except:
                # Final hail mary for encoding issues
                try:
                    safe_text = text.encode('latin-1', 'ignore').decode('latin-1')
                    page.insert_text((x, y), safe_text, fontname="helv", fontsize=block['size'])
                except: pass

    def _draw_images(self, page: fitz.Page, p_num: Any, img_map: Dict):
        """
        Injects raw image bytes (JP2/TIFF) directly into the PDF.
        """
        # Filter images belonging to this page
        p_imgs = [c for c in img_map.values() 
                 if str(c.get('metadata', {}).get('page_num')) == str(p_num)]
        
        for img in p_imgs:
            bbox = img.get('metadata', {}).get('bbox')
            if bbox:
                try:
                    # insert_image detects the format (JP2/TIFF) automatically
                    page.insert_image(
                        fitz.Rect(bbox), 
                        stream=img['data']
                    )
                except Exception as e:
                    logger.warning(f"Image injection failed: {e}")

__all__ = ["PDFRebuilder"]