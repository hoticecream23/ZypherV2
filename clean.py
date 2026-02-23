from pathlib import Path
import shutil

# Folders to clean
FOLDERS = ["output", "restored"]

def clean_folder(folder_path: Path):
    if not folder_path.exists():
        print(f"[skip] {folder_path} does not exist")
        return
    
    for item in folder_path.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        except Exception as e:
            print(f"[error] Failed to delete {item}: {e}")
    
    print(f"[cleaned] {folder_path}")

def main():
    root = Path(__file__).parent
    
    for folder in FOLDERS:
        clean_folder(root / folder)

if __name__ == "__main__":
    main()
