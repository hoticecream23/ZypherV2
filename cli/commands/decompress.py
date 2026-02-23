"""
Zypher CLI - Decompress Command
Usage: python -m zypher_cli.commands.decompress input.zpkg -o output_folder
"""

import argparse
import sys
import os
import shutil
from pathlib import Path
from core.unpacker.unpacker import Unpacker
from core.utils.logger import logger

def main():
    parser = argparse.ArgumentParser(description="Decompress a Zypher archive and rebuild the original file")
    parser.add_argument("input", help="Path to the .zpkg file")
    parser.add_argument("-o", "--output", help="Directory to save the restored file")
    parser.add_argument("-f", "--force", action="store_true", help="Overwrite existing files")
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Archive not found: {input_path}")
        sys.exit(1)

    # Determine Output Directory
    if args.output:
        out_dir = Path(args.output)
    else:
        # Default to a folder named after the file in the current dir
        # e.g., docs/file.zpkg -> ./file_restored/
        out_dir = Path.cwd() / f"{input_path.stem}_restored"

    if out_dir.exists() and not args.force:
        logger.error(f"Output directory exists: {out_dir}")
        print("Use -f or --force to overwrite.")
        sys.exit(1)
        
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(f"Starting decompression: {input_path.name}")
        
        # Initialize Engine
        unpacker = Unpacker()
        
        # Run Unpack
        result = unpacker.unpack(str(input_path), str(out_dir))
        
        if result['success']:
            print(f"\n✅ Success! Restored to: {result['output_path']}")
        else:
            logger.error("Decompression failed.")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user.")
        shutil.rmtree(out_dir, ignore_errors=True)
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        # Clean up failed directory
        if out_dir.exists() and not any(out_dir.iterdir()):
            out_dir.rmdir()
        sys.exit(1)

if __name__ == "__main__":
    main()