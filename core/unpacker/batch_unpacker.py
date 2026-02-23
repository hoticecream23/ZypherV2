"""
Zypher Batch Unpacker
Decompresses multiple .zpkg files concurrently with progress tracking.
"""
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Callable
from ..utils.logger import logger
from .unpacker import Unpacker
from ..config import config


class BatchUnpacker:
    def __init__(
        self,
        dict_path: str = None,
        max_workers: int = None
    ):
        self.max_workers = max_workers or min(32, (os.cpu_count() or 4) * 2)

        # Single Unpacker instance shared across threads
        # Dictionary loaded once into memory, reused for all files
        self.unpacker = Unpacker(dict_path=dict_path)

    def decompress_directory(
        self,
        input_dir: str,
        output_dir: str,
        recursive: bool = False,
        on_progress: Optional[Callable] = None
    ) -> Dict:
        """
        Decompress all .zpkg files in a directory.

        Args:
            input_dir: directory containing .zpkg files
            output_dir: directory to restore original files into
            recursive: if True, walks subdirectories
            on_progress: optional callback(completed, total, result)

        Returns:
            summary dict with results, stats, and failures
        """
        input_path = Path(input_dir)
        if not input_path.exists():
            raise ValueError(f"Input directory not found: {input_dir}")

        if recursive:
            files = list(input_path.rglob('*.zpkg'))
        else:
            files = list(input_path.glob('*.zpkg'))

        if not files:
            logger.warning(f"No .zpkg files found in {input_dir}")
            return self._empty_summary()

        # Build output paths mirroring input structure
        jobs = []
        for f in files:
            if recursive:
                relative = f.relative_to(input_path)
                out_dir = Path(output_dir) / relative.parent
            else:
                out_dir = Path(output_dir)
            jobs.append((f, out_dir))

        logger.info(f"🔓 Batch decompressing {len(jobs)} files with {self.max_workers} workers...")
        return self._run(jobs, on_progress)

    def decompress_files(
        self,
        file_paths: List[str],
        output_dir: str,
        on_progress: Optional[Callable] = None
    ) -> Dict:
        """
        Decompress a specific list of .zpkg files.

        Args:
            file_paths: list of .zpkg file paths
            output_dir: directory to restore files into
            on_progress: optional callback(completed, total, result)
        """
        jobs = [
            (Path(f), Path(output_dir))
            for f in file_paths
        ]
        logger.info(f"🔓 Batch decompressing {len(jobs)} files with {self.max_workers} workers...")
        return self._run(jobs, on_progress)

    def _run(
        self,
        jobs: List[tuple],
        on_progress: Optional[Callable]
    ) -> Dict:
        start_time = time.time()
        results = []
        failures = []
        total = len(jobs)
        completed = 0

        # Ensure all output directories exist upfront
        for _, out_dir in jobs:
            out_dir.mkdir(parents=True, exist_ok=True)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_job = {
                executor.submit(self._decompress_one, inp, out_dir): (inp, out_dir)
                for inp, out_dir in jobs
            }

            for future in as_completed(future_to_job):
                inp, out_dir = future_to_job[future]
                completed += 1

                try:
                    result = future.result()
                    results.append(result)

                    if on_progress:
                        on_progress(completed, total, result)

                    logger.info(
                        f"   [{completed}/{total}] {inp.name} "
                        f"→ {result['output_path']}"
                    )

                except Exception as e:
                    failure = {
                        'file': str(inp),
                        'error': str(e)
                    }
                    failures.append(failure)

                    if on_progress:
                        on_progress(completed, total, failure)

                    logger.error(f"   [{completed}/{total}] {inp.name} — {e}")

        elapsed = time.time() - start_time
        return self._build_summary(results, failures, elapsed)

    def _decompress_one(self, input_path: Path, output_dir: Path) -> Dict:
        """Decompress a single file — called from thread pool"""
        result = self.unpacker.unpack(
            str(input_path),
            str(output_dir)
        )
        result['input_file'] = str(input_path)
        return result

    def _build_summary(self, results: List[Dict], failures: List[Dict], elapsed: float) -> Dict:
        total = len(results) + len(failures)
        total_restored = sum(r.get('original_size', 0) for r in results)

        logger.info(f"\n{'='*50}")
        logger.info(f"✨ Batch Decompression Complete!")
        logger.info(f"   Files:     {len(results)} succeeded, {len(failures)} failed")
        logger.info(f"   Restored:  {total_restored/1024/1024:.2f} MB")
        logger.info(f"   Time:      {elapsed:.2f}s")
        logger.info(f"{'='*50}")

        return {
            'success': len(failures) == 0,
            'total': total,
            'succeeded': len(results),
            'failed': len(failures),
            'failures': failures,
            'results': results,
            'total_restored_size': total_restored,
            'processing_time': elapsed
        }

    def _empty_summary(self) -> Dict:
        return {
            'success': True,
            'total': 0,
            'succeeded': 0,
            'failed': 0,
            'failures': [],
            'results': [],
            'total_restored_size': 0,
            'processing_time': 0
        }


__all__ = ["BatchUnpacker"]