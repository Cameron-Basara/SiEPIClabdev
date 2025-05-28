import threading
import heapq
import time

class PriorityCommand:
    def __init__(self, priority, action, name=""):
        self.priority = priority
        self.action = action  # A callable (function)
        self.name = name
        self.timestamp = time.time()

    def __lt__(self, other):
        # Higher priority first. If equal, earlier timestamp wins
        return (-self.priority, self.timestamp) < (-other.priority, other.timestamp)

class CommandQueue:
    def __init__(self):
        self.heap = []
        self.lock = threading.Lock()
        self.event = threading.Event()

    def push(self, command):
        with self.lock:
            heapq.heappush(self.heap, command)
            self.event.set()  # Notify worker that something is available

    def pop(self):
        with self.lock:
            if self.heap:
                return heapq.heappop(self.heap)
            else:
                self.event.clear()
                return None
    def print_queue(self):
         with self.lock:
            print("\n[Current Queue]")
            for idx, cmd in enumerate(self.heap):
                print(f"  {idx+1}: {cmd.name} (priority {cmd.priority})")
            print("[End of Queue]\n")

class Worker(threading.Thread):
    def __init__(self, queue):
        super().__init__(daemon=True)
        self.queue = queue
        self.running = True

    def run(self):
        while self.running:
            self.queue.event.wait()  # Sleep until something is available
            self.queue.print_queue()
            cmd = self.queue.pop()
            if cmd:
                print(f"Running command {cmd.name} with priority {cmd.priority}")
                cmd.action()
            else:
                time.sleep(0.01)  # Just in case of race conditions

    def stop(self):
        self.running = False
        self.queue.event.set()  # Wake up if sleeping

# Example commands
def example_task(name):
    print(f"Executing {name}...")
    time.sleep(1)
    print(f"{name} Done.")

# Usage
queue = CommandQueue()
worker = Worker(queue)
worker.start()

# Add some commands
queue.push(PriorityCommand(priority=5, action=lambda: example_task("Low priority"), name="Laser scan"))
queue.push(PriorityCommand(priority=10, action=lambda: example_task("High priority"), name="Move camera"))

# Let it run for a while
time.sleep(10)
worker.stop()
