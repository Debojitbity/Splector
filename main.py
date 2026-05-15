import subprocess
import sys
import time
from pathlib import Path

def run_script(script_path):
    """Runs a python script using subprocess and returns its return code."""
    script_name = Path(script_path).name
    print(f"\n{'='*60}")
    print(f"STARTING: {script_name}")
    print(f"{'='*60}\n")
    
    try:
        # Using sys.executable to ensure we use the same python interpreter
        process = subprocess.run([sys.executable, script_path], check=True)
        print(f"\n{'='*60}")
        print(f"COMPLETED: {script_name}")
        print(f"{'='*60}\n")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n{'!'*60}")
        print(f"ERROR: {script_name} failed with return code {e.returncode}")
        print(f"{'!'*60}\n")
        return e.returncode
    except Exception as e:
        print(f"\n{'!'*60}")
        print(f"ERROR: An unexpected error occurred while running {script_name}: {e}")
        print(f"{'!'*60}\n")
        return 1

def main():
    print("Initializing Splector Domain Analysis Workflow...")
    
    # 1. Run Domain Checker
    checker_path = "src/domain_checker.py"
    exit_code = run_script(checker_path)
    if exit_code != 0:
        print("Workflow aborted due to failure in Domain Checker.")
        sys.exit(exit_code)
    
    # Small pause between scripts
    time.sleep(1)
    
    # 2. Run Domain Grader
    grader_path = "src/domain_grader.py"
    exit_code = run_script(grader_path)
    if exit_code != 0:
        print("Workflow aborted due to failure in Domain Grader.")
        sys.exit(exit_code)
    
    print("\nSplector Workflow Completed Successfully. Exiting gracefully.")
    sys.exit(0)

if __name__ == "__main__":
    main()
