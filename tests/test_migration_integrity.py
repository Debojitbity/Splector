import sqlite3
import pandas as pd
import urllib.parse
from pathlib import Path

def test_migration_integrity():
    base_dir = Path(__file__).resolve().parent.parent
    excel_path = base_dir / "data" / "links.xlsx"
    db_path = base_dir / "data" / "crawler.db"

    assert excel_path.exists(), "Excel file not found!"
    assert db_path.exists(), "Database file not found!"

    print("Verifying Migration Integrity...")

    # Count rows in Excel
    excel_counts = {}
    total_excel_rows = 0
    sheets = ["production", "temporary", "unstable"]
    seen_domains = set()
    
    excel = pd.ExcelFile(excel_path, engine="openpyxl")
    for sheet in sheets:
        if sheet in excel.sheet_names:
            df = pd.read_excel(excel_path, sheet_name=sheet, engine="openpyxl")
            df = df.dropna(subset=["domain"])
            
            # We must account for the uniqueness logic used during migration
            sheet_unique_count = 0
            for _, row in df.iterrows():
                clean_domain = urllib.parse.unquote(str(row["domain"]).strip(), encoding='utf-8')
                if clean_domain not in seen_domains:
                    seen_domains.add(clean_domain)
                    sheet_unique_count += 1
                    
            excel_counts[sheet] = sheet_unique_count
            total_excel_rows += sheet_unique_count

    # Count rows in SQLite
    sqlite_counts = {}
    total_sqlite_rows = 0
    with sqlite3.connect(db_path) as conn:
        for sheet in sheets:
            count = conn.execute("SELECT COUNT(*) FROM domains WHERE source_sheet = ?", (sheet,)).fetchone()[0]
            sqlite_counts[sheet] = count
            total_sqlite_rows += count
        
        actual_total = conn.execute("SELECT COUNT(*) FROM domains").fetchone()[0]

    # Display results
    print("\n[Row Counts by Sheet]")
    for sheet in sheets:
        if sheet in excel_counts:
            print(f"Sheet '{sheet}': Excel = {excel_counts[sheet]}, SQLite = {sqlite_counts[sheet]}")
            assert excel_counts[sheet] == sqlite_counts[sheet], f"Mismatch in {sheet} sheet!"

    print(f"\nTotal Expected Unique (Excel): {total_excel_rows}")
    print(f"Total Inserted (SQLite): {total_sqlite_rows}")
    print(f"Actual Table Count: {actual_total}")

    assert total_excel_rows == total_sqlite_rows, "Total count mismatch!"
    assert total_sqlite_rows == actual_total, "Mismatch in overall count!"

    print("\nREADY TO DELETE EXCEL")

if __name__ == "__main__":
    test_migration_integrity()
