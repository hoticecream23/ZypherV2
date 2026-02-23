"""
Zypher PDF Rebuilder - Production Grade
Fixes:
  #1: Font trigger list too broad (was matching custom fonts like Roboto, OpenSans)
  #2: Wrong PyMuPDF font code ("ti" -> "tiro")
  #3: draw_line() wrong arguments (4 floats -> 2 fitz.Points)
  #4: shape.finish() called before drawing with wrong params
  #5: Academic PDF ligature/encoding garble detection and skip
  #6: SAFE_MODE=True was killing all custom fonts
"""

import fitz
import json
import zstandard as zstd
from typing import List, Dict, Any, Optional
from ..utils.logger import logger


class PDFRebuilder:

    # FIX #6: Was True - killed all custom font logic
    SAFE_MODE = False

    # FIX #1: Exact full normalized names only, not loose substrings
    # 'roman' would match 'GaramondRoman', 'sans' would match 'OpenSans', etc.
    STANDARD_FONT_EXACT = {
        # Times family
        'timesnewroman', 'timesnewromanps', 'timesnewromanpsmt',
        'timesroman', 'timesbold', 'timesitalic', 'timesbolditalic',
        'times-roman', 'times-bold', 'times-italic', 'times-bolditalic',
        # Helvetica / Arial family
        'helvetica', 'helveticaneue', 'helveticabold', 'helveticaoblique',
        'arial', 'arialbold', 'arialitalic', 'arialmt', 'arialboldmt',
        # Courier family
        'courier', 'courierbold', 'courieroblique', 'couriernew', 'couriernewps',
        # Standard symbols
        'symbol', 'zapfdingbats',
        # Microsoft standard
        'calibri', 'calibribold', 'calibriitalic', 'calibribolditalic',
        # TeX Computer Modern (academic PDFs - LaTeX)
        
        #'cmr10', 'cmr12', 'cmr9', 'cmr8', 'cmr7', 'cmr6',
        #'cmmi10', 'cmmi12', 'cmmi9', 'cmmi8', 'cmmi7',
        #'cmsy10', 'cmsy9', 'cmsy8', 'cmex10',
        #'cmbx10', 'cmbx12', 'cmbx9', 'cmtt10', 'cmtt12',
        #'cmsl10', 'cmti10', 'cmbxti10',
    
    }

    def rebuild(self, chunks: List[Dict], output_path: str, manifest: Dict, package_path: str = None) -> None:
        try:
            logger.info(f"Rebuilding: {output_path}")
            doc = fitz.open()
            layout = manifest.get('page_layouts', {})
            if not layout:
                logger.warning("No layout data - falling back to text dump")
                self._rebuild_fallback(chunks, output_path)
                return
            img_map = {c['id']: c for c in chunks if c['type'] == 'image'}
            vec_map = {c['id']: c for c in chunks if c['type'] == 'vectors'}
            font_buffers = {}
            if not self.SAFE_MODE:
                font_buffers = self._load_fonts(chunks)
            try:
                sorted_pages = sorted(layout.keys(), key=lambda k: int(str(k)))
            except (ValueError, TypeError):
                sorted_pages = sorted(layout.keys())
            
            for p_num in sorted_pages:
                p_data = layout[p_num]
                if p_data.get('raster_fallback') and package_path:
                    self._copy_original_page(doc, package_path, int(p_num) - 1)
                else:
                    page = doc.new_page(
                        width=p_data.get('width', 612),
                        height=p_data.get('height', 792)
                    )
                    if v_chunk := next(
                        (v for v in vec_map.values()
                        if str(v.get('metadata', {}).get('page_num')) == str(p_num)),
                        None
                    ):
                        self._draw_vectors(page, v_chunk)
                    self._draw_text(page, p_data.get('blocks', []), font_buffers, p_data.get('height', 792))
                    self._draw_images(page, p_num, img_map)
            
            doc.save(output_path, deflate=True)
            doc.close()
            logger.info(f"PDF rebuilt: {output_path}")
        except Exception as e:
            logger.error(f"Rebuild failed: {e}", exc_info=True)
            raise

    def _copy_original_page(self, dst_doc: fitz.Document, package_path: str, page_index: int):
        """Copy original page directly from zpkg — perfect fidelity"""
        import zipfile
        import tempfile
        import os

        try:
            with zipfile.ZipFile(package_path, 'r') as z:
                names = z.namelist()
                pdf_files = [n for n in names if n.startswith('original/') and n.endswith('.pdf')]

                if not pdf_files:
                    logger.warning(f"No original PDF found in package")
                    return

                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                    tmp.write(z.read(pdf_files[0]))
                    tmp_path = tmp.name

            src_doc = fitz.open(tmp_path)
            dst_doc.insert_pdf(src_doc, from_page=page_index, to_page=page_index)
            src_doc.close()
            os.unlink(tmp_path)

        except Exception as e:
            logger.warning(f"Direct page copy failed for page {page_index + 1}: {e}")

    def _load_fonts(self, chunks: List[Dict]) -> Dict[str, bytes]:
        font_buffers = {}
        font_chunk = next((c for c in chunks if c['type'] == 'fonts'), None)

        if font_chunk:
            try:
                dctx = zstd.ZstdDecompressor()
                f_json_bytes = dctx.decompress(font_chunk['data'])
                font_data_map = json.loads(f_json_bytes.decode('utf-8'))

                if isinstance(font_data_map, str):
                    font_data_map = json.loads(font_data_map)

                for name, hex_data in font_data_map.items():
                    try:
                        font_buffers[name] = bytes.fromhex(hex_data)
                        if '+' in name:
                            font_buffers[name.split('+')[1]] = font_buffers[name]
                    except ValueError as e:
                        logger.warning(f"Font '{name}' has invalid hex data, skipping: {e}")

                logger.info(f"Loaded {len(font_data_map)} embedded fonts")
            except Exception as e:
                logger.warning(f"Font load error: {e}")

        return font_buffers

    def _is_standard_font(self, font_name: str) -> bool:
        """
        FIX #1: Exact normalized name match.
        Strips subset prefix (ABCDEF+FontName -> fontname)
        and normalizes dashes/spaces before comparing.
        """
        name = font_name.lower()
        if '+' in name:
            name = name.split('+')[1]

        # Normalize: remove dashes, spaces, commas
        normalized = name.replace('-', '').replace(' ', '').replace(',', '')

        return normalized in self.STANDARD_FONT_EXACT or name in self.STANDARD_FONT_EXACT

    def _get_base14_code(self, font_name: str, flags: int = 0) -> str:
        """
        FIX #2: Correct PyMuPDF Base14 codes.
        Previous code used "ti" which is invalid - correct is "tiro".
        """
        name = font_name.lower()
        if '+' in name:
            name = name.split('+')[1]

        is_bold = bool(flags & 16) or any(x in name for x in ['bold', 'bd', 'black', 'heavy'])
        is_italic = bool(flags & 2) or any(x in name for x in ['italic', 'oblique', 'slant', 'it'])

        # Courier / Monospace family
        if any(x in name for x in ['courier', 'mono', 'typewriter', 'cmtt']):
            if is_bold and is_italic: return 'cobi'
            if is_bold:               return 'cobo'
            if is_italic:             return 'coit'
            return 'cour'

        # Times / Serif family (including TeX CMR, CMMI)
        if any(x in name for x in ['times', 'cmr', 'cmmi', 'cmsl', 'cmti',
                                     'garamond', 'georgia', 'palatino', 'bookman']):
            if is_bold and is_italic: return 'tibi'
            if is_bold:               return 'tibo'
            if is_italic:             return 'tiit'
            return 'tiro'  # FIX #2: was "ti"

        # Symbol fonts (TeX CMSY, CMEX)
        if any(x in name for x in ['symbol', 'cmsy', 'cmex', 'dingbat']):
            return 'symb'

        # Default: Helvetica / Sans family
        if is_bold and is_italic: return 'hebi'
        if is_bold:               return 'hebo'
        if is_italic:             return 'heit'
        return 'helv'

    def _draw_text(self, page: fitz.Page, blocks: List, font_buffers: Dict, page_height: float = 792):
        
        logger.info(f"Available font_buffers keys: {list(font_buffers.keys())[:5]}")
        #logger.warning(f"_draw_text called: {len(blocks)} blocks, page_height={page_height}")
        """
        FIX #1, #2, #5, #6: Smart font selection.
        - Exact standard font matching (not loose substrings)
        - Correct Base14 codes
        - Custom font binary injection
        - Garbled text detection (academic PDFs)
        """
        registered_on_page = set()

        font_buffers_lower = {k.lower(): v for k, v in font_buffers.items()}

        for block in blocks:
            text = block.get('text', '').strip()
            if not text:
                continue

            orig_name = block.get('font', '')
            flags = block.get('flags', 0)
            size = block.get('size', 10)
            x = block.get('x', 0)
            y = block.get('y', 0) + block.get('size', 10)

            orig_name_lower = orig_name.lower()
            clean_name_lower = orig_name_lower.split('+')[1] if '+' in orig_name_lower else orig_name_lower

            non_ascii = sum(1 for c in text if ord(c) > 127 or c in '\x00\ufffd')
            if len(text) > 3 and (non_ascii / len(text)) > 0.4:
                continue

            font_face = None

            if self._is_standard_font(orig_name):
                font_face = self._get_base14_code(orig_name, flags)
            else:
                target_key = orig_name_lower if orig_name_lower in font_buffers_lower else \
                            clean_name_lower if clean_name_lower in font_buffers_lower else None

                if target_key:
                    original_case_key = next(k for k in font_buffers if k.lower() == target_key)
                    font_face = original_case_key
                    if original_case_key not in registered_on_page:
                        try:
                            page.insert_font(          # <-- HERE
                                fontname=original_case_key,
                                fontbuffer=font_buffers[original_case_key]
                            )
                            registered_on_page.add(original_case_key)
                            logger.info(f"Font injected OK: {original_case_key}")
                        except Exception as e:
                            logger.warning(f"Custom font inject failed ({original_case_key}): {e} — falling back to Base14")
                            font_face = self._get_base14_code(orig_name, flags)
                else:
                    font_face = self._get_base14_code(orig_name, flags)

            try:
                page.insert_text((x, y), text, fontname=font_face, fontsize=size)
            except Exception:
                try:
                    safe_text = text.encode('latin-1', 'ignore').decode('latin-1')
                    if safe_text.strip():
                        page.insert_text((x, y), safe_text, fontname='helv', fontsize=size)
                except Exception:
                    pass

    def _draw_full_page_image(self, page: fitz.Page, p_num: Any, img_map: Dict):
        """Insert full page raster for pages with undecodable fonts"""
        p_imgs = [
            c for c in img_map.values()
            if str(c.get('metadata', {}).get('page_num')) == str(p_num)
            and c.get('metadata', {}).get('is_full_page', False)
        ]
        for img in p_imgs:
            try:
                rect = fitz.Rect(0, 0, page.rect.width, page.rect.height)
                page.insert_image(rect, stream=img['data'])
                return
            except Exception as e:
                logger.warning(f"Full page image insert failed: {e}")

    def _draw_vectors(self, page: fitz.Page, v_chunk: Dict):
        """
        FIX #3, #4: Correct draw_line args and shape.finish() placement/params.
        """
        try:
            raw = v_chunk.get('data', b'')
            if isinstance(raw, bytes):
                try:
                    dctx = zstd.ZstdDecompressor()
                    v_json = dctx.decompress(raw)
                except Exception:
                    v_json = raw
                vectors = json.loads(v_json)
            else:
                vectors = raw

            shape = page.new_shape()

            for obj in vectors:
                stroke_color = obj.get('color')
                fill_color = obj.get('fill')
                line_width = float(obj.get('width', 1.0))

                # Draw all path items FIRST
                drew_something = False
                for item in obj.get('items', []):
                    cmd = item[0]

                    if cmd == "l":
                        p1, p2 = item[1], item[2]
                        # FIX #3: fitz.Point objects, not 4 raw floats
                        shape.draw_line(
                            fitz.Point(p1['x'], p1['y']),
                            fitz.Point(p2['x'], p2['y'])
                        )
                        drew_something = True

                    elif cmd == "re":
                        r = item[1]
                        shape.draw_rect(fitz.Rect(r['x0'], r['y0'], r['x1'], r['y1']))
                        drew_something = True

                    elif cmd == "c":
                        p1, p2, p3, p4 = item[1], item[2], item[3], item[4]
                        shape.draw_bezier(
                            fitz.Point(p1['x'], p1['y']),
                            fitz.Point(p2['x'], p2['y']),
                            fitz.Point(p3['x'], p3['y']),
                            fitz.Point(p4['x'], p4['y'])
                        )
                        drew_something = True

                # FIX #4: Call finish() AFTER drawing, with correct API params
                if drew_something:
                    shape.finish(
                        color=stroke_color,  # stroke color
                        fill=fill_color,     # fill color (None = no fill)
                        width=line_width,    # line width
                        closePath=False
                    )

            shape.commit()

        except Exception as e:
            logger.warning(f"Vector draw failed: {e}")

    def _draw_images(self, page: fitz.Page, p_num: Any, img_map: Dict):
        p_imgs = [
            c for c in img_map.values()
            if str(c.get('metadata', {}).get('page_num')) == str(p_num)
        ]
        for img in p_imgs:
            bbox = img.get('metadata', {}).get('bbox')
            if bbox:
                try:
                    page.insert_image(fitz.Rect(bbox), stream=img['data'])
                except Exception as e:
                    logger.warning(f"Image inject failed: {e}")

    def _rebuild_fallback(self, chunks: List[Dict], output_path: str):
        """Plain text fallback when no layout data"""
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter

        c = canvas.Canvas(output_path, pagesize=letter)
        y = 750
        c.setFont("Helvetica", 10)

        for chunk in [ch for ch in chunks if ch['type'] == 'text']:
            data = chunk['data']
            text = data.decode('utf-8', errors='ignore') if isinstance(data, bytes) else str(data)
            for line in text.split('\n'):
                if y < 50:
                    c.showPage()
                    y = 750
                    c.setFont("Helvetica", 10)
                c.drawString(50, y, line[:100])
                y -= 12
        c.save()


__all__ = ["PDFRebuilder"]