from process_utils import kill_python_processes
import sys

if __name__ == "__main__":
    # clean zombie subprocesses
    kill_python_processes(filter_substring="python")
    print(f"Cleanup complete.")