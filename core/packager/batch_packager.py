"""
Zypher Batch Packager
Compresses multiple files concurrently with progress tracking.
"""
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Callable
from ..utils.logger import logger
from .packager import Packager
from ..config import config


class BatchPackager:
    def __init__(
        self,
        dict_path: str = None,
        max_workers: int = None,
        compression_level: str = 'high',
        max_file_size_mb: int = 500,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self.compression_level = compression_level
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        # Default workers — I/O bound so more than CPU count is fine
        self.max_workers = max_workers or min(32, (os.cpu_count() or 4) * 2)

        # Single Packager instance shared across threads
        # Dictionary loaded once into memory, reused for all files
        self.packager = Packager(dict_path=dict_path, max_file_size_mb=max_file_size_mb)

    def compress_directory(
        self,
        input_dir: str,
        output_dir: str,
        recursive: bool = False,
        on_progress: Optional[Callable] = None
    ) -> Dict:
        """
        Compress all supported files in a directory.

        Args:
            input_dir: directory containing files to compress
            output_dir: directory to write .zpkg files
            recursive: if True, walks subdirectories
            on_progress: optional callback(completed, total, result) called after each file

        Returns:
            summary dict with results, stats, and failures
        """
        input_path = Path(input_dir)
        if not input_path.exists():
            raise ValueError(f"Input directory not found: {input_dir}")

        # Collect files
        if recursive:
            files = [
                f for f in input_path.rglob('*')
                if f.is_file() and f.suffix.lower() in self.packager.SUPPORTED_FORMATS
            ]
        else:
            files = [
                f for f in input_path.iterdir()
                if f.is_file() and f.suffix.lower() in self.packager.SUPPORTED_FORMATS
            ]

        if not files:
            logger.warning(f"No supported files found in {input_dir}")
            return self._empty_summary()

        # Build output paths mirroring input structure
        jobs = []
        for f in files:
            if recursive:
                relative = f.relative_to(input_path)
                out = Path(output_dir) / relative.parent / (f.stem + '.zpkg')
            else:
                out = Path(output_dir) / (f.stem + '.zpkg')
            jobs.append((f, out))

        logger.info(f" Batch compressing {len(jobs)} files with {self.max_workers} workers...")
        return self._run(jobs, on_progress)

    def compress_files(
        self,
        file_paths: List[str],
        output_dir: str,
        on_progress: Optional[Callable] = None
    ) -> Dict:
        """
        Compress a specific list of files.

        Args:
            file_paths: list of file paths to compress
            output_dir: directory to write .zpkg files
            on_progress: optional callback(completed, total, result)
        """
        jobs = [
            (Path(f), Path(output_dir) / (Path(f).stem + '.zpkg'))
            for f in file_paths
        ]
        logger.info(f" Batch compressing {len(jobs)} files with {self.max_workers} workers...")
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

        # Ensure all output directories exist
        for _, out in jobs:
            out.parent.mkdir(parents=True, exist_ok=True)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_job = {
                executor.submit(self._compress_one, inp, out): (inp, out)
                for inp, out in jobs
            }

            for future in as_completed(future_to_job):
                inp, out = future_to_job[future]
                completed += 1

                try:
                    result = future.result()
                    results.append(result)

                    if on_progress:
                        on_progress(completed, total, result)

                    logger.info(
                        f"   [{completed}/{total}] {inp.name} "
                        f"— {result['space_saved_percent']:.1f}% saved"
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

    def _compress_one(self, input_path: Path, output_path: Path) -> Dict:
        result = self.packager.compress_with_retry(
            str(input_path),
            str(output_path),
            compression_level=self.compression_level,
            on_progress=None,
            max_retries=self.max_retries,
            retry_delay=self.retry_delay
        )
        result['input_file'] = str(input_path)
        return result

    def _build_summary(self, results: List[Dict], failures: List[Dict], elapsed: float) -> Dict:
        total = len(results) + len(failures)
        total_original = sum(r['original_size'] for r in results)
        total_compressed = sum(r['compressed_size'] for r in results)
        overall_saving = (
            (1 - total_compressed / total_original) * 100
            if total_original > 0 else 0
        )

        logger.info(f"\n{'='*50}")
        logger.info(f" Batch Complete!")
        logger.info(f"   Files:     {len(results)} succeeded, {len(failures)} failed")
        logger.info(f"   Original:  {total_original/1024/1024:.2f} MB")
        logger.info(f"   Zypher:    {total_compressed/1024/1024:.2f} MB")
        logger.info(f"   Saved:     {overall_saving:.1f}%")
        logger.info(f"   Time:      {elapsed:.2f}s")
        logger.info(f"{'='*50}")

        return {
            'success': len(failures) == 0,
            'total': total,
            'succeeded': len(results),
            'failed': len(failures),
            'failures': failures,
            'results': results,
            'total_original_size': total_original,
            'total_compressed_size': total_compressed,
            'overall_saving_percent': overall_saving,
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
            'total_original_size': 0,
            'total_compressed_size': 0,
            'overall_saving_percent': 0,
            'processing_time': 0
        }


__all__ = ["BatchPackager"]