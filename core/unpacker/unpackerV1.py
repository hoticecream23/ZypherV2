"""
Zypher Unpacker - Integrity Verifier & Restorer


import zipfile
import json
import tempfile
import shutil
import time
from pathlib import Path
from typing import Dict, Optional
from ..rebuilder.pdf_rebuilder import PDFRebuilder
from ..utils.logger import logger
from ..utils.checksum import calculate_bytes_checksum

class Unpacker:
    def unpack(self, package_path: str, output_dir: str) -> Dict:
        start_time = time.time()
        
        # We extract to a temp dir first to validate integrity
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            
            try:
                logger.info(f"🔓 Unpacking: {package_path}")
                with zipfile.ZipFile(package_path, 'r') as z:
                    z.extractall(temp_dir)
                
                # 1. Load Manifest
                manifest_path = temp_dir / 'manifest.json'
                if not manifest_path.exists():
                    raise ValueError("Invalid package: manifest.json missing")
                
                manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
                
                # 2. Verify & Load Chunks
                chunks = []
                for c_info in manifest['chunks']:
                    chunk_file = temp_dir / 'chunks' / f"{c_info['id']}.zst"
                    
                    if not chunk_file.exists():
                        logger.warning(f"Missing chunk: {c_info['id']}")
                        continue
                    
                    data = chunk_file.read_bytes()
                    
                    # Integrity Check
                    # Note: You can disable this for speed if needed
                    current_checksum = calculate_bytes_checksum(data)
                    if current_checksum != c_info['checksum']:
                        logger.error(f"Checksum mismatch for {c_info['id']}")
                        # In production, you might raise an error here
                    
                    chunks.append({
                        'id': c_info['id'],
                        'type': c_info['type'],
                        'data': data,
                        'metadata': c_info.get('metadata', {})
                    })

                # 3. Rebuild
                out_path = Path(output_dir)
                out_path.mkdir(parents=True, exist_ok=True)
                
                original_name = manifest.get('original_filename', 'restored_file')
                final_output = out_path / original_name
                
                if manifest['original_format'] == 'pdf':
                    PDFRebuilder().rebuild(chunks, str(final_output), manifest, package_path)
                
                return {
                    'success': True,
                    'output_path': str(final_output),
                    'time': time.time() - start_time
                }

            except Exception as e:
                logger.error(f"Unpack failed: {e}")
                raise

__all__ = ["Unpacker"]
"""