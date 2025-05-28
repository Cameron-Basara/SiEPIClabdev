from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import random

def log_status(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def measure_device(device_name):
    log_status(f"[{threading.current_thread().name}] Starting measurement on {device_name}")
    time.sleep(random.uniform(1.5, 3.0))  
    log_status(f"[{threading.current_thread().name}] Finished measurement on {device_name}")
    return f"Result-{device_name}"

def calibrate_device(device_name):
    log_status(f"[{threading.current_thread().name}] Starting calibration on {device_name}")
    time.sleep(2)
    log_status(f"[{threading.current_thread().name}] Finished calibration on {device_name}")
    return f"Calibration-{device_name}"

# Initialize the pool with a limited number of threads
executor = ThreadPoolExecutor(max_workers=4)

# Submit initial measurement tasks
device_names = ['SMU1', 'LaserA', 'CameraX']
future_to_device = {executor.submit(measure_device, dev): dev for dev in device_names}

# Wait a bit before submitting a new task (simulating a new signal or event)
time.sleep(1)
log_status("New calibration request received!")
cal_future = executor.submit(calibrate_device, 'PiezoY')

# Add the calibration task to our futures list to track it too
future_to_device[cal_future] = 'PiezoY'

# Wait for all tasks to complete
for future in as_completed(future_to_device):
    result = future.result()
    log_status(f"Task done: {result}")
