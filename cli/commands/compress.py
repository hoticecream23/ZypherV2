"""
Zypher CLI - Compress Command
Usage: python -m cli.commands.compress input.pdf -o output.zpkg
"""
import argparse
import sys
from pathlib import Path
from core.utils.logger import logger


def main():
    parser = argparse.ArgumentParser(description="Compress a file into a Zypher archive (.zpkg)")
    parser.add_argument("input", help="Path to the input file (PDF, Image)")
    parser.add_argument("-o", "--output", help="Path to the output .zpkg file")
    parser.add_argument("-l", "--level", choices=['low', 'medium', 'high', 'ultra'], default='high',
                        help="Compression level (default: high)")
    parser.add_argument("-m", "--mode", choices=['lossless', 'visual'],
                        default='lossless', help="Compression mode (default: lossless)")

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_suffix('.zpkg')

    output_path.parent.mkdir(parents=True, exist_ok=True)

    def on_progress(bytes_done, total):
        if total < 10 * 1024 * 1024:
            return
        percent = bytes_done / total * 100
        filled = int(percent / 2)
        bar = '█' * filled + '░' * (50 - filled)
        mb_done = bytes_done / 1024 / 1024
        mb_total = total / 1024 / 1024
        sys.stdout.write(f'\r   [{bar}] {percent:.1f}% ({mb_done:.1f}/{mb_total:.1f} MB)')
        sys.stdout.flush()
        if bytes_done == total:
            sys.stdout.write('\n')

    try:
        logger.info(f"Starting compression: {input_path.name} [{args.mode}]")

        if args.mode == 'visual':
            ext = input_path.suffix.lower()
            if ext != '.pdf':
                logger.error("Visual mode only supports PDF files")
                sys.exit(1)
            from core.packager.visual_packager import VisualPackager
            packager = VisualPackager()
            
        else:
            from core.packager.packager import Packager
            packager = Packager()

        #packager = Packager(mode=args.mode)

        result = packager.compress_file(
            str(input_path),
            str(output_path),
            compression_level=args.level,
            on_progress=on_progress
        )

        if result['success']:
            mode_label = "visual" if args.mode == 'visual' else "lossless"
            print(f"\n✅ Success! [{mode_label}] Archive saved to: {output_path}")
            print(f"   Saved:     {result['space_saved_percent']:.1f}%")
            print(f"   Ratio:     {result['compression_ratio']:.2f}x smaller")
            print(f"   Time:      {result['processing_time']:.2f}s")
        else:
            logger.error("Compression failed without raising exception.")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        if output_path.exists():
            output_path.unlink()
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()