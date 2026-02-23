"""
Zypher PDF Extractor
Extracts text from PDFs for search indexing.
Called at upload time, results stored in search database.
"""
import fitz
from typing import Dict
from ..utils.logger import logger


class PDFExtractor:
    def extract_text_for_search(self, pdf_path: str) -> Dict:
        """
        Extracts text from each page for search indexing.
        """
        result = {
            'pages': [],
            'full_text': '',
            'page_count': 0
        }

        try:
            doc = fitz.open(pdf_path)
            full_text = []

            for page_index in range(len(doc)):
                page = doc[page_index]
                text = page.get_text("text").strip()

                result['pages'].append({
                    'page_num': page_index + 1,
                    'text': text
                })
                full_text.append(text)

            result['full_text'] = '\n'.join(full_text)
            result['page_count'] = len(doc)
            doc.close()

            logger.info(f"Extracted text: {result['page_count']} pages")

        except Exception as e:
            logger.error(f"Text extraction failed: {e}", exc_info=True)

        return result


__all__ = ["PDFExtractor"]