import pdfplumber

with pdfplumber.open('input/GCON.pdf') as pdf:
    page = pdf.pages[0]
    words = page.extract_words()
    
    # Show first 10 words
    print("First 10 words from pdfplumber:")
    for w in words[:10]:
        print(f"  '{w['text']}' at x={w['x0']:.1f}, y={w['top']:.1f}")