import os
import sqlite3

# --- Configuration ---
DB_PATH = "data/crawler.db"
PHASE2_DIR = "data/phase2_prepared"

def clean_disguised_pdfs():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Fetch all supposedly successful text files
    cursor.execute("""
        SELECT record_id, prepared_file_path 
        FROM document_refs 
        WHERE processing_status = 'SUCCESS'
    """)
    rows = cursor.fetchall()

    corrupted_count = 0

    print("Scanning database for corrupted Disguised PDFs...")

    for row in rows:
        record_id, file_path = row
        
        if file_path and os.path.exists(file_path):
            try:
                # Open with errors='ignore' because binary data will crash standard utf-8 decoding
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    header = f.read(100)
                    
                    # Check for the binary PDF signature
                    if '%PDF' in header:
                        # 1. Delete the corrupted text file
                        f.close()
                        os.remove(file_path)
                        
                        # 2. Delete the DB record so the deduplication moat lets it through again
                        cursor.execute("DELETE FROM document_refs WHERE record_id = ?", (record_id,))
                        corrupted_count += 1
                        
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

    conn.commit()
    conn.close()

    print("Purge Complete! 🧹")
    print(f"Deleted {corrupted_count} corrupted PDFs.")
    print("Their database records have been wiped so Splector can re-process them correctly.")

if __name__ == "__main__":
    clean_disguised_pdfs()