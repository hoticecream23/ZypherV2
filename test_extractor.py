
from core.extractor.extractor import Extractor

extractor = Extractor()

# Test with your sample PDF
result = extractor.extract_text_for_search("input/sample.pdf")

print(f"Status: {result['status']}")
print(f"Format: {result['format']}")
print(f"Pages: {result['page_count']}")
print(f"Text preview: {result['full_text']}")

'''
from core.extractor.extractor import Extractor

extractor = Extractor()

# Test streaming - yields one page at a time
for page in extractor.extract_pages_streaming("input/sample.pdf"):
    print(f"Page {page['page_num']}: {page['text'][:100]}")
'''