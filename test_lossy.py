from core.packager.lossy_packager import LossyPackager

packager = LossyPackager(jpeg_quality=85)
result = packager.compress_file("input/scan.pdf", "output/scan.zpkg")