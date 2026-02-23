"""
Zypher CLI - Batch Decompress Command
Usage: python -m cli.commands.decompress_batch input_dir/ -o output_dir/ [options]
"""
import argparse
import sys
from pathlib import Path
from core.unpacker.batch_unpacker import BatchUnpacker
from core.utils.logger import logger


def main():
    parser = argparse.ArgumentParser(description="Batch decompress Zypher archives")
    parser.add_argument("input", help="Input directory containing .zpkg files")
    parser.add_argument("-o", "--output", help="Output directory for restored files")
    parser.add_argument("-r", "--recursive", action="store_true",
                        help="Recursively decompress subdirectories")
    parser.add_argument("-w", "--workers", type=int, default=None,
                        help="Number of parallel workers (default: auto)")
    parser.add_argument("-f", "--force", action="store_true",
                        help="Overwrite existing files in output directory")

    args = parser.parse_args()

    input_dir = Path(args.input)
    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        sys.exit(1)

    output_dir = (
        Path(args.output) if args.output
        else input_dir.parent / (input_dir.name + '_restored')
    )

    if output_dir.exists() and any(output_dir.iterdir()) and not args.force:
        logger.error(f"Output directory is not empty: {output_dir}")
        print("Use -f or --force to overwrite existing files.")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        batch = BatchUnpacker(max_workers=args.workers)

        summary = batch.decompress_directory(
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            recursive=args.recursive
        )

        if summary['failed'] > 0:
            print(f"\n{summary['failed']} file(s) failed:")
            for f in summary['failures']:
                print(f"   {f['file']}: {f['error']}")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nBatch operation cancelled.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()