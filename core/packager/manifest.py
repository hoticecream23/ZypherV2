'''
"""
Zypher Manifest Generator
Standardizes the .zpkg metadata structure
"""
import time
from typing import List, Dict, Any

class ZypherManifest:
    VERSION = "2.0"

    def create_manifest(
        self,
        filename: str,
        file_type: str,
        original_size: int,
        compressed_size: int,
        chunks: List[Dict],
        encrypted: bool,
        metadata: Dict[str, Any]
    ) -> Dict:
        return {
            "version": self.VERSION,
            "original_filename": filename,
            "original_format": file_type,
            "created_at": time.time(),
            "original_size": original_size,
            "compressed_size": compressed_size, # Placeholder, updated after zip
            "chunk_count": len(chunks),
            "encrypted": encrypted,
            "metadata": metadata,
            "chunks": chunks,
            # 'page_layouts' will be injected here dynamically by the packager
        }

    def save_manifest(self, manifest: Dict, path: str):
        import json
        with open(path, 'w') as f:
            json.dump(manifest, f)

__all__ = ["ZypherManifest"]
'''
"""
Zypher Manifest Generator
Standardizes the .zpkg metadata structure
"""
import time
from typing import List, Dict, Any


class ZypherManifest:
    VERSION = "2.0"
    
    def create_manifest(
        self,
        filename: str,
        file_type: str,
        original_size: int,
        compressed_size: int,
        chunks: List[Dict],
        encrypted: bool,
        metadata: Dict[str, Any]
    ) -> Dict:
        return {
            "version": self.VERSION,
            "original_filename": filename,
            "original_format": file_type,
            "created_at": time.time(),
            "original_size": original_size,
            "compressed_size": compressed_size,
            "chunk_count": len(chunks),
            "encrypted": encrypted,
            "metadata": metadata,
            "chunks": chunks,
        }
    
    def save_manifest(self, manifest: Dict, path: str):
        import json
        # FIX: Added encoding='utf-8' and ensure_ascii=False for international chars
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False)
    
    def load_manifest(self, manifest_path: str) -> Dict:
        """Load manifest from JSON file"""
        import json
        # FIX: Added encoding='utf-8' for Windows compatibility
        with open(manifest_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def add_chunk(
        self,
        chunk_id: str,
        chunk_type: str,
        algorithm: str,
        original_size: int,
        compressed_size: int,
        checksum: str,
        metadata: Dict = None
    ) -> Dict:
        """Helper to create chunk entry"""
        return {
            'id': chunk_id,
            'type': chunk_type,
            'algorithm': algorithm,
            'original_size': original_size,
            'compressed_size': compressed_size,
            'checksum': checksum,
            'metadata': metadata or {}
        }
    
    def validate_manifest(self, manifest: Dict) -> tuple:
        """Basic validation - returns (is_valid, error_message)"""
        required_keys = ['version', 'original_filename', 'chunks']
        for key in required_keys:
            if key not in manifest:
                return False, f"Missing required key: {key}"
        return True, None


__all__ = ["ZypherManifest"]