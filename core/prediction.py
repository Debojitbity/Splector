import os
import sqlite3
import joblib
from pathlib import Path

# ==========================================
# 1. CONFIGURATION PATHS
# ==========================================
MODEL_DIR = Path(r"C:\Users\Dev\Desktop\Projects\Splector\Models")
DATA_DIR = Path(r"C:\Users\Dev\Desktop\Projects\Splector\data\phase2_prepared")
DB_PATH = r"C:\Users\Dev\Desktop\Projects\Splector\data\crawler.db"

SVM_PATH = MODEL_DIR / "svm_model.pkl"
TFIDF_PATH = MODEL_DIR / "tfidf_vectorizer.pkl"

# Optional: Adjust this if you are using a custom threshold for 0% False Negatives
DECISION_THRESHOLD = 0.90  

def setup_database():
    """Initializes the new predictions table in the main database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS document_predictions (
            file_name TEXT PRIMARY KEY,
            is_job TEXT,
            confidence_score REAL,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn

def run_batch_prediction():
    print("Loading Machine Learning Engine...")
    try:
        vectorizer = joblib.load(TFIDF_PATH)
        classifier = joblib.load(SVM_PATH)
    except FileNotFoundError as e:
        print(f"Error loading models: {e}")
        return

    print("Scanning phase2_prepared directory for .txt files...")
    if not DATA_DIR.exists():
        print(f"Directory not found: {DATA_DIR}")
        return

    # Collect all text data
    file_names = []
    raw_texts = []
    
    for filepath in DATA_DIR.glob("*.txt"):
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                raw_texts.append(f.read())
                file_names.append(filepath.name)
        except Exception as e:
            print(f"Failed to read {filepath.name}: {e}")

    if not raw_texts:
        print("No .txt files found to process.")
        return

    print(f"Vectorizing {len(raw_texts)} documents...")
    X_tfidf = vectorizer.transform(raw_texts)
    
    print("Running classification...")
    # Using decision_function to get the raw distance from the hyperplane (Confidence Score)
    decision_scores = classifier.decision_function(X_tfidf)
    
    # Prepare data for SQL insertion
    db_records = []
    for file_name, score in zip(file_names, decision_scores):
        # Convert NumPy float to standard Python float for SQLite compatibility
        confidence = float(score)
        
        # Apply threshold to determine "Yes" (Job) or "No" (Non-Job)
        is_job = "Yes" if confidence > DECISION_THRESHOLD else "No"
        
        db_records.append((file_name, is_job, confidence))

    print("Saving predictions to crawler.db...")
    conn = setup_database()
    cursor = conn.cursor()
    
    # Use REPLACE to update the record if the file was already predicted previously
    cursor.executemany("""
        INSERT OR REPLACE INTO document_predictions (file_name, is_job, confidence_score)
        VALUES (?, ?, ?)
    """, db_records)
    
    conn.commit()
    conn.close()
    
    print("Pipeline Complete. Database updated successfully.")

if __name__ == "__main__":
    run_batch_prediction()