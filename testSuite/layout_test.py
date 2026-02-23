import zipfile, json, zstandard as zstd

with zipfile.ZipFile('output/GCON.zpkg', 'r') as z:
    manifest = json.loads(z.read('manifest.json').decode('utf-8'))
    print("page_layouts in manifest?", 'page_layouts' in manifest)
    
    if 'page_layouts' in manifest:
        layout = manifest['page_layouts']
        first_page = layout[list(layout.keys())[0]]
        print(f"\nFirst page blocks: {len(first_page['blocks'])}")
        print("First 2 blocks:")
        for b in first_page['blocks'][:2]:
            print(f"  '{b['text'][:50]}' at x={b['x']}, y={b['y']}")