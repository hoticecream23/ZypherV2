"""
Zypher CLI - Inspect Command
Usage: python -m cli.commands.inspect output/doc.zpkg
"""
import argparse
import sys
from pathlib import Path
from core.tools.inspector import Inspector
from core.utils.logger import logger


def main():
    parser = argparse.ArgumentParser(
        description="Inspect a Zypher archive without decompressing"
    )
    parser.add_argument("input", help="Path to the .zpkg archive")

    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        logger.error(f"Archive not found: {path}")
        sys.exit(1)

    try:
        Inspector().inspect(str(path))
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Inspection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()