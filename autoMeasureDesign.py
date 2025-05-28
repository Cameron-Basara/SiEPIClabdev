import threading
import time

# -------------------
# JOB CLASS
# -------------------

class AutoMeasureJob:
    def __init__(self, laser, motorOpt, motorElec, smu, fineAlign, graph):
        self.laser = laser
        self.motorOpt = motorOpt
        self.motorElec = motorElec
        self.smu = smu
        self.fineAlign = fineAlign
        self.graphPanel = graph
        self.running = threading.Event()
        self.abort = threading.Event()

    def run(self):
        print("[AutoMeasureJob] Starting measurements...")
        self.running.set()

        while self.running.is_set():
            if self.abort.is_set():
                print("[AutoMeasureJob] Abort detected. Stopping.")
                break

            # --- Your real measurement logic here ---
            print("[AutoMeasureJob] Performing a measurement step...")
            time.sleep(1.0)  # Simulate doing something
            # --- End of logic ---

        print("[AutoMeasureJob] Finished or Aborted.")

    def stop(self):
        print("[AutoMeasureJob] Stopping politely...")
        self.running.clear()

    def force_abort(self):
        print("[AutoMeasureJob] Forcing immediate abort...")
        self.abort.set()
        self.running.clear()

# -------------------
# MANAGER CLASS
# -------------------

class AutoMeasureManager:
    def __init__(self, job):
        self.job = job
        self.thread = None

    def start(self):
        if self.thread is None or not self.thread.is_alive():
            print("[AutoMeasureManager] Starting new thread...")
            self.thread = threading.Thread(target=self.job.run, daemon=True)
            self.thread.start()
        else:
            print("[AutoMeasureManager] Thread already running.")

    def stop(self):
        print("[AutoMeasureManager] Requesting polite stop...")
        self.job.stop()
        if self.thread:
            self.thread.join()

    def abort(self):
        print("[AutoMeasureManager] Requesting immediate abort...")
        self.job.force_abort()
        if self.thread:
            self.thread.join()

# -------------------
# MAIN SCRIPT
# -------------------

if __name__ == "__main__":
    # Fake hardware for testing
    laser = "FakeLaser"
    motorOpt = "FakeMotorOpt"
    motorElec = "FakeMotorElec"
    smu = "FakeSMU"
    fineAlign = "FakeAlign"
    graph = "FakeGraph"

    # Create job and manager
    job = AutoMeasureJob(laser, motorOpt, motorElec, smu, fineAlign, graph)
    manager = AutoMeasureManager(job)

    # Example usage
    manager.start()

    time.sleep(3)  # Let it run a bit...

    print("\n--- Asking to politely stop ---\n")
    manager.stop()

    print("\n--- Restarting and forcing abort ---\n")
    manager.start()
    time.sleep(2)
    manager.abort()

    print("\n[Main] Program finished.")
