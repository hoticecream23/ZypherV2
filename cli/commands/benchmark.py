"""
Zypher CLI - Benchmark Command
Usage: python -m cli.commands.benchmark input/doc.pdf
"""
import argparse
import sys
from pathlib import Path
from core.tools.benchmark import Benchmark
from core.utils.logger import logger


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark all compression levels and modes against a file"
    )
    parser.add_argument("input", help="Path to the file to benchmark")

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"File not found: {input_path}")
        sys.exit(1)

    try:
        bench = Benchmark()
        bench.run(str(input_path))

    except KeyboardInterrupt:
        print("\n❌ Benchmark cancelled.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()