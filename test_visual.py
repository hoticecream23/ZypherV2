# test_visual.py
from core.packager.visual_packager import VisualPackager

packager = VisualPackager(jpeg_quality=85)
result = packager.compress_file("input/img.pdf", "output/img_visual.zpkg")
print(f"Saved: {result['space_saved_percent']:.1f}%")
print(f"Original: {result['original_size']/1024:.1f}KB")
print(f"Compressed: {result['compressed_size']/1024:.1f}KB")
