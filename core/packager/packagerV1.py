'''
v1
"""
Zypher Packager - High Efficiency Core
Orchestrates the Smart Compression Pipeline:
1. Force Layout Extraction (pdfplumber) -> Golden Copy of Text Positions
2. Smart Image Compression (JP2/TIFF) -> Content-Aware Optimization
3. Streamed Processing -> Low Memory Footprint
"""

import zipfile
import os
import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional, List, Any

# Import our new High-Efficiency Modules
from ..extractor import PDFExtractor
from ..compressor import TextCompressor, ImageCompressor, MetadataCompressor
from ..utils.logger import logger
from ..utils.checksum import calculate_bytes_checksum
from .manifest import ZypherManifest 

class Packager:
    """
    The Main Engine. 
    Connects Extractors -> Compressors -> Archiver.
    """
    
    def __init__(self):
        # Initialize the Engines
        self.pdf_extractor = PDFExtractor()
        self.text_comp = TextCompressor()      # Zstd Level 22
        self.img_comp = ImageCompressor()      # Smart JP2/TIFF selector
        self.meta_comp = MetadataCompressor()  # Layout Engine
        self.manifest_gen = ZypherManifest()

    def compress_file(
        self,
        input_path: str,
        output_path: str,
        compression_level: str = 'high' # defaults to Ultra mode
    ) -> Dict:
        """
        Compresses a file into a .zpkg archive using Content-Aware Optimization.
        """
        start_time = time.time()
        temp_dir = Path(tempfile.mkdtemp(prefix="zypher_pkg_"))
        chunks_dir = temp_dir / 'chunks'
        chunks_dir.mkdir()

        try:
            logger.info(f"📦 Packaging: {input_path}")
            original_size = os.path.getsize(input_path)
            
            manifest_chunks = []
            global_layout = None
            
            # --- PHASE 1: FORCE LAYOUT EXTRACTION (The "Golden Map") ---
            # We do this first to ensure we have the perfect coordinate system.
            if input_path.lower().endswith('.pdf'):
                logger.info("   🔍 Extracting high-fidelity layout (pdfplumber)...")
                global_layout = self.meta_comp.extract_layout(input_path)
                
                if global_layout:
                    # We compress the layout map itself as a chunk for backup/integrity
                    l_bytes = self.meta_comp.compress(global_layout)
                    l_id = "layout_global"
                    (chunks_dir / f"{l_id}.zst").write_bytes(l_bytes)
                    
                    manifest_chunks.append({
                        'id': l_id,
                        'type': 'layout',
                        'algorithm': 'zstd',
                        'original_size': len(json.dumps(global_layout)),
                        'compressed_size': len(l_bytes),
                        'checksum': calculate_bytes_checksum(l_bytes),
                        'metadata': {'format': 'json', 'source': 'pdfplumber'}
                    })
                    logger.info(f"   ✅ Layout map secured ({len(global_layout)} pages)")
            
            # --- PHASE 2: CONTENT STREAMING (The "Asset Pipeline") ---
            logger.info("   🚀 Streaming content assets...")

            # Initialize the manifest components
            manifest_chunks = []
            page_layouts = {}  # <--- ADD THIS LINE TO FIX THE NameError
            doc_metadata = {}

            logger.info("   🚀 Streaming content assets...")
            
            # ... rest of your loop ...
            
            # Use the streaming extractor to keep memory usage low
            for item in self.pdf_extractor.extract_streaming(input_path):
                
                # A. Handle Metadata
                if item['type'] == 'metadata':
                    doc_metadata = item.get('metadata', {})
                    continue

                # C. Handle Fonts (NEW)
                if item['type'] == 'font_map':
                    fonts = item.get('fonts', {})
                    if fonts:
                        # Compress the entire font map structure
                        # We use MetadataCompressor (Zstd) because it's a Dict of bytes
                        # Note: We serialize the dict structure, Zstd handles binary strings well
                        import pickle # Use pickle for binary-heavy dicts
                        
                        f_bytes = self.meta_comp.compress(json.dumps({
                            k: v.hex() for k, v in fonts.items() # Hex encode binaries for JSON safety
                        }))
                        
                        f_id = "fonts_global"
                        (chunks_dir / f"{f_id}.zst").write_bytes(f_bytes)
                        
                        manifest_chunks.append({
                            'id': f_id,
                            'type': 'fonts',
                            'algorithm': 'zstd_hex', # Mark as hex-encoded
                            'original_size': len(f_bytes), # Approx
                            'compressed_size': len(f_bytes),
                            'checksum': calculate_bytes_checksum(f_bytes)
                        })
                        logger.info(f"   ✅ Captured {len(fonts)} embedded fonts")
                    continue
                """
                # B. Handle Pages
                if item['type'] == 'page':
                    page_num = item['page_num']
                    
                    # 1. Compress Images (Smart Segmentation)
                    for i, img in enumerate(item.get('images', [])):
                        # Pass raw bytes to Smart Compressor (decides JP2 vs TIFF)
                        comp_data = self.img_comp.compress(img['data'])
                        
                        cid = f"img_{page_num}_{i}"
                        (chunks_dir / f"{cid}.zst").write_bytes(comp_data)
                        
                        # Calculate savings for logging
                        orig_len = len(img['data'])
                        comp_len = len(comp_data)
                        ratio = (1 - (comp_len / orig_len)) * 100 if orig_len > 0 else 0
                        
                        manifest_chunks.append({
                            'id': cid,
                            'type': 'image',
                            'algorithm': 'smart_content_aware', # Marks it for the Rebuilder
                            'original_size': orig_len,
                            'compressed_size': comp_len,
                            'checksum': calculate_bytes_checksum(comp_data),
                            'metadata': {
                                'page_num': page_num,
                                'bbox': img.get('bbox'),
                                'ext': img.get('ext')
                            }
                        })
                """
                if item['type'] == 'page':
                    page_num = item['page_num']
                    
                    # 1. Capture High-Fidelity Layout (NEW)
                    # We store the cleaned blocks directly from the unified extractor
                    page_layouts[page_num] = {
                        'width': item['width'],
                        'height': item['height'],
                        'blocks': item.get('blocks', []) # Crucial for fixing gibberish
                    }
                    
                    # 2. Compress Images (Existing Logic)
                    for i, img in enumerate(item.get('images', [])):
                        comp_data = self.img_comp.compress(img['data'])
                        cid = f"img_{page_num}_{i}"
                        (chunks_dir / f"{cid}.zst").write_bytes(comp_data)
                        
                        orig_len = len(img['data'])
                        comp_len = len(comp_data)
                        
                        manifest_chunks.append({
                            'id': cid,
                            'type': 'image',
                            'algorithm': 'smart_content_aware',
                            'original_size': orig_len,
                            'compressed_size': comp_len,
                            'checksum': calculate_bytes_checksum(comp_data),
                            'metadata': {
                                'page_num': page_num,
                                'bbox': img.get('bbox'),
                                'ext': img.get('ext')
                            }
                        })

                    if item.get('vectors'):
                        # Vectors are list/dict data, so we use MetadataCompressor (Zstd)
                        v_bytes = self.meta_comp.compress(item['vectors'])
                        v_id = f"vec_{page_num}"
                        (chunks_dir / f"{v_id}.zst").write_bytes(v_bytes)
                        
                        manifest_chunks.append({
                            'id': v_id,
                            'type': 'vectors', # Rebuilder looks for this
                            'algorithm': 'zstd',
                            'original_size': len(json.dumps(item['vectors'])),
                            'compressed_size': len(v_bytes),
                            'checksum': calculate_bytes_checksum(v_bytes),
                            'metadata': {'page_num': page_num}
                        })

            # --- PHASE 3: FINALIZATION (The "Container") ---
            
            # Generate Manifest
            # We inject the global_layout directly into the manifest for the Rebuilder
            # to access instantly without needing to decompress a chunk.
            manifest = self.manifest_gen.create_manifest(
                filename=Path(input_path).name,
                file_type='pdf',
                original_size=original_size,
                compressed_size=0, # Calculated by zip
                chunks=manifest_chunks,
                encrypted=False,
                metadata=doc_metadata if 'doc_metadata' in locals() else {}
            )
            
            if global_layout:
                manifest['page_layouts'] = global_layout
                manifest['has_formatting_data'] = True

            # Write Manifest
            (temp_dir / 'manifest.json').write_text(json.dumps(manifest))

            # Zip it all up
            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as z:
                z.write(temp_dir / 'manifest.json', 'manifest.json')
                z.writestr('.zpkg_signature', 'ZYPHER_v2_HIGH_EFFICIENCY')
                
                for f in chunks_dir.glob('*.zst'):
                    z.write(f, f"chunks/{f.name}")

            # Metrics
            final_size = os.path.getsize(output_path)
            saved = original_size - final_size
            percent = (saved / original_size) * 100 if original_size > 0 else 0
            
            logger.info(f"✨ Compression Complete!")
            logger.info(f"   Original: {original_size/1024:.2f} KB")
            logger.info(f"   Zypher:   {final_size/1024:.2f} KB")
            logger.info(f"   Saved:    {percent:.1f}%")
            
            return {
                'success': True,
                'output_file': output_path,
                'compression_ratio': final_size / original_size if original_size else 1.0
            }

        except Exception as e:
            logger.error(f"Packaging failed: {e}", exc_info=True)
            if os.path.exists(output_path):
                os.remove(output_path)
            raise
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

__all__ = ["Packager"]

"""
Zypher Packager - Complete Fixed Version
Integrates all fixes:
- MetadataCompressor for pdfplumber layout with PyMuPDF text correction
- Proper chunk storage and manifest integration
- Font extraction and storage
"""

import zipfile
import os
import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional, List, Any

from ..extractor import PDFExtractor
from ..compressor import TextCompressor, ImageCompressor, MetadataCompressor
from ..utils.logger import logger
from ..utils.checksum import calculate_bytes_checksum
from .manifest import ZypherManifest 


class Packager:
    """Main compression engine with full layout preservation"""
    
    def __init__(self):
        self.pdf_extractor = PDFExtractor()
        self.text_comp = TextCompressor()
        self.img_comp = ImageCompressor()
        self.meta_comp = MetadataCompressor()
        self.manifest_gen = ZypherManifest()

    def compress_file(
        self,
        input_path: str,
        output_path: str,
        compression_level: str = 'high'
    ) -> Dict:
        """Compresses file with full layout preservation"""
        start_time = time.time()
        temp_dir = Path(tempfile.mkdtemp(prefix="zypher_pkg_"))
        chunks_dir = temp_dir / 'chunks'
        chunks_dir.mkdir()

        try:
            logger.info(f"📦 Packaging: {input_path}")
            original_size = os.path.getsize(input_path)
            
            manifest_chunks = []
            global_layout = None
            doc_metadata = {}
            
            # PHASE 1: Extract layout with MetadataCompressor
            # This uses pdfplumber coords + PyMuPDF text (fixes encoding issues)
            if input_path.lower().endswith('.pdf'):
                logger.info("   🔍 Extracting high-fidelity layout (hybrid mode)...")
                global_layout = self.meta_comp.extract_layout(input_path)
                
                if global_layout:
                    # Compress and store layout chunk
                    l_bytes = self.meta_comp.compress(global_layout)
                    l_id = "layout_global"
                    (chunks_dir / f"{l_id}.zst").write_bytes(l_bytes)
                    
                    manifest_chunks.append({
                        'id': l_id,
                        'type': 'layout',
                        'algorithm': 'zstd',
                        'original_size': len(json.dumps(global_layout, ensure_ascii=False)),
                        'compressed_size': len(l_bytes),
                        'checksum': calculate_bytes_checksum(l_bytes),
                        'metadata': {'format': 'json', 'source': 'hybrid_plumber_fitz'}
                    })
                    logger.info(f"   ✅ Layout map secured ({len(global_layout)} pages)")

            # PHASE 2: Stream content (images, text, fonts)
            logger.info("   🚀 Streaming content assets...")
            
            for item in self.pdf_extractor.extract_streaming(input_path):
                
                # Handle metadata
                if item['type'] == 'metadata':
                    doc_metadata = item.get('metadata', {})
                    continue

                # Handle fonts
                if item['type'] == 'font_map':
                    fonts = item.get('fonts', {})
                    if fonts:
                        # Fonts are already hex-encoded by extractor
                        f_bytes = self.meta_comp.compress(fonts)
                        f_id = "fonts_global"
                        (chunks_dir / f"{f_id}.zst").write_bytes(f_bytes)
                        
                        manifest_chunks.append({
                            'id': f_id,
                            'type': 'fonts',
                            'algorithm': 'zstd',
                            'original_size': len(json.dumps(fonts)),
                            'compressed_size': len(f_bytes),
                            'checksum': calculate_bytes_checksum(f_bytes),
                            'metadata': {'format': 'hex_dict'}
                        })
                        logger.info(f"   ✅ Captured {len(fonts)} embedded fonts")
                    continue
                
                # Handle pages
                if item['type'] == 'page':
                    page_num = item['page_num']
                    
                    # Compress images
                    for i, img in enumerate(item.get('images', [])):
                        img_data = img['data']
                        
                        # Use smart compression
                        try:
                            comp_data = self.img_comp.compress(img_data)
                        except Exception as e:
                            logger.debug(f"Image compression failed, storing raw: {e}")
                            comp_data = img_data
                        
                        cid = f"img_{page_num}_{i}"
                        (chunks_dir / f"{cid}.zst").write_bytes(comp_data)
                        
                        manifest_chunks.append({
                            'id': cid,
                            'type': 'image',
                            'algorithm': 'smart',
                            'original_size': len(img_data),
                            'compressed_size': len(comp_data),
                            'checksum': calculate_bytes_checksum(comp_data),
                            'metadata': {
                                'page_num': page_num,
                                'bbox': img.get('bbox'),
                                'format': img.get('format', 'unknown'),
                                'is_full_page': img.get('is_full_page', False)  # ADD THIS
                            }
                        })
                    
                    # Compress vectors if present
                    if item.get('vectors'):
                        v_bytes = self.meta_comp.compress(item['vectors'])
                        v_id = f"vec_{page_num}"
                        (chunks_dir / f"{v_id}.zst").write_bytes(v_bytes)
                        
                        manifest_chunks.append({
                            'id': v_id,
                            'type': 'vectors',
                            'algorithm': 'zstd',
                            'original_size': len(json.dumps(item['vectors'])),
                            'compressed_size': len(v_bytes),
                            'checksum': calculate_bytes_checksum(v_bytes),
                            'metadata': {'page_num': page_num}
                        })

            # PHASE 3: Create manifest
            manifest = self.manifest_gen.create_manifest(
                filename=Path(input_path).name,
                file_type='pdf',
                original_size=original_size,
                compressed_size=sum(c['compressed_size'] for c in manifest_chunks),
                chunks=manifest_chunks,
                encrypted=False,
                metadata=doc_metadata
            )
            
            # CRITICAL: Add layout to manifest so rebuilder can access it
            if global_layout:
                manifest['page_layouts'] = global_layout
                manifest['has_formatting_data'] = True
                logger.info(f"   ✅ Layout added to manifest")

            # Write manifest
            manifest_path = temp_dir / 'manifest.json'
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )

            needs_original = any(
                p.get('raster_fallback')
                for p in global_layout.values()
            ) if global_layout else False

            # Create final ZIP
            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as z:
                z.write(manifest_path, 'manifest.json')
                z.writestr('.zpkg_signature', 'ZYPHER_v2_HYBRID')
                z.write(input_path, f"original/{Path(input_path).name}")  # ADD THIS
                
                for f in chunks_dir.glob('*.zst'):
                    z.write(f, f"chunks/{f.name}")

            # Metrics
            final_size = os.path.getsize(output_path)
            saved = original_size - final_size
            percent = (saved / original_size) * 100 if original_size > 0 else 0
            elapsed = time.time() - start_time
            
            logger.info(f"✨ Compression Complete!")
            logger.info(f"   Original: {original_size/1024:.2f} KB")
            logger.info(f"   Zypher:   {final_size/1024:.2f} KB")
            logger.info(f"   Saved:    {percent:.1f}%")
            logger.info(f"   Time:     {elapsed:.2f}s")
            
            return {
                'success': True,
                'output_file': output_path,
                'original_size': original_size,
                'compressed_size': final_size,
                'compression_ratio': final_size / original_size if original_size else 1.0,
                'space_saved_percent': percent,
                'processing_time': elapsed
            }

        except Exception as e:
            logger.error(f"Packaging failed: {e}", exc_info=True)
            if os.path.exists(output_path):
                os.remove(output_path)
            raise
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

__all__ = ["Packager"]
'''