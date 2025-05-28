from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import random


executor = ThreadPoolExecutor(max_workers=10)
shutdown_flag = threading.Event()

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def background_task(name):
    log(f"[{threading.current_thread().name}] Started task: {name}")
    time.sleep(random.uniform(1, 3))
    log(f"[{threading.current_thread().name}] Finished task: {name}")
    return f"Result of {name}"

def input_listener():
    while not shutdown_flag.is_set():
        try:
            user_input = input("Enter a new task name (or type 'exit' to quit): ")
            if user_input.strip().lower() == 'exit':
                shutdown_flag.set()
                break
            # Submit the new task to the thread pool
            future = executor.submit(background_task, user_input)
        except EOFError:
            # In case of Ctrl+D or broken input
            shutdown_flag.set()
            break

# Start a background thread to handle input
input_thread = threading.Thread(target=input_listener, daemon=True)
input_thread.start()

try:
    # Keep main thread alive while executor is working
    while not shutdown_flag.is_set():
        time.sleep(0.5)
except KeyboardInterrupt:
    shutdown_flag.set()

log("Shutting down...")
executor.shutdown(wait=True)
log("All tasks completed.")
