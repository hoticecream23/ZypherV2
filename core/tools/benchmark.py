"""
Zypher Benchmark Tool
Tests all compression levels and modes against a file
and reports results side by side.
"""
import os
import time
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict
from ..utils.logger import logger


class Benchmark:
    LEVELS = ['low', 'medium', 'high', 'ultra']

    def run(self, input_path: str) -> Dict:
        """
        Run all levels and modes against a file.
        Returns full results and prints a comparison table.
        """
        input_path = Path(input_path)
        if not input_path.exists():
            raise ValueError(f"File not found: {input_path}")

        ext = input_path.suffix.lower()
        original_size = os.path.getsize(input_path)
        results = []

        # Create temp dir for benchmark outputs
        tmp_dir = Path(tempfile.mkdtemp(prefix='zypher_bench_'))

        try:
            # Run lossless mode for all levels
            from ..packager.packager import Packager
            for level in self.LEVELS:
                result = self._run_one(
                    input_path=input_path,
                    output_dir=tmp_dir,
                    level=level,
                    mode='lossless',
                    original_size=original_size
                )
                results.append(result)

            # Run visual mode for PDFs only
            if ext == '.pdf':
                from ..packager.visual_packager import VisualPackager
                for level in self.LEVELS:
                    result = self._run_one(
                        input_path=input_path,
                        output_dir=tmp_dir,
                        level=level,
                        mode='visual',
                        original_size=original_size
                    )
                    results.append(result)

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        self._print_table(input_path.name, original_size, results)

        return {
            'file': str(input_path),
            'original_size': original_size,
            'results': results
        }

    def _run_one(
        self,
        input_path: Path,
        output_dir: Path,
        level: str,
        mode: str,
        original_size: int
    ) -> Dict:
        output_path = output_dir / f"{input_path.stem}_{mode}_{level}.zpkg"

        try:
            start = time.time()

            if mode == 'visual':
                from ..packager.visual_packager import VisualPackager
                packager = VisualPackager()
            else:
                from ..packager.packager import Packager
                packager = Packager()

            result = packager.compress_file(
                str(input_path),
                str(output_path),
                compression_level=level
            )

            elapsed = time.time() - start
            compressed_size = os.path.getsize(output_path)

            return {
                'mode': mode,
                'level': level,
                'compressed_size': compressed_size,
                'space_saved_percent': (1 - compressed_size / original_size) * 100,
                'compression_ratio': original_size / compressed_size,
                'time': elapsed,
                'success': True
            }

        except Exception as e:
            return {
                'mode': mode,
                'level': level,
                'compressed_size': 0,
                'space_saved_percent': 0,
                'compression_ratio': 0,
                'time': 0,
                'success': False,
                'error': str(e)
            }

    def _print_table(self, filename: str, original_size: int, results: List[Dict]):
        print(f"\n{'='*65}")
        print(f"  Zypher Benchmark: {filename}")
        print(f"  Original size:    {original_size/1024:.1f} KB")
        print(f"{'='*65}")
        print(f"  {'Mode':<12} {'Level':<10} {'Size':>10} {'Saved':>8} {'Ratio':>8} {'Time':>8}")
        print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*8} {'-'*8} {'-'*8}")

        current_mode = None
        for r in results:
            if r['mode'] != current_mode:
                current_mode = r['mode']
                print()

            if not r['success']:
                print(f"  {r['mode']:<12} {r['level']:<10} {'ERROR':>10}")
                continue

            size_kb = r['compressed_size'] / 1024
            saved = r['space_saved_percent']
            ratio = r['compression_ratio']
            elapsed = r['time']

            # Highlight best result
            marker = ' ◀' if saved == max(x['space_saved_percent'] for x in results) else ''

            print(
                f"  {r['mode']:<12} {r['level']:<10} "
                f"{size_kb:>9.1f}K "
                f"{saved:>7.1f}% "
                f"{ratio:>7.2f}x "
                f"{elapsed:>7.2f}s"
                f"{marker}"
            )

        best = max(results, key=lambda x: x['space_saved_percent'])
        print(f"\n{'='*65}")
        print(f"  Best: {best['mode']} / {best['level']} — {best['space_saved_percent']:.1f}% saved")
        print(f"{'='*65}\n")