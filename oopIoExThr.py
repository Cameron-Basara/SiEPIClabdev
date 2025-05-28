from concurrent.futures import ThreadPoolExecutor
import threading
import heapq
import time
import random

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

    def push(self, task):
        with self.lock:
            heapq.heappush(self.heap, task)
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

class TaskManager:
    def __init__(self, max_workers=1, queue = CommandQueue, pr = PriorityCommand, priority = 7):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.shutdown_flag = threading.Event()
        self.lock = threading.Lock()
        self.task_id = 0
        self.queue = queue()
        self.pr = PriorityCommand
        self.priority = priority

    def log(self, msg):
        print(f"[{time.strftime('%H:%M:%S')}] {msg}")

    def background_task(self, task_name):
        thread_name = threading.current_thread().name
        self.log(f"[{thread_name}] Started task: {task_name}")
        time.sleep(random.uniform(1, 3))
        self.log(f"[{thread_name}] Finished task: {task_name}")
        return f"Result of {task_name}"

    def submit_task(self, task_name, task, priority):
        with self.lock:
            self.task_id += 1
            full_name = f"{task_name}_{self.task_id}"
            self.queue.push(self.pr(priority=priority, action=lambda name=full_name: task(name), name=full_name))
            
    def next_task(self):
        with self.lock:
            self.queue.print_queue()
            cmd = self.queue.pop()
            if cmd is not None:
                future = self.executor.submit(cmd.action)
                return future

    def input_loop(self):
        while not self.shutdown_flag.is_set():
            try:
                user_input = input("Enter a new task name (or 'exit' to quit): ").strip()
                task = self.background_task
                if user_input.lower() == "exit":
                    self.shutdown_flag.set()
                    break
                self.submit_task(user_input, task, priority=self.priority)
                self.next_task()
            except EOFError:
                self.shutdown_flag.set()
                break

    def run(self):
        input_thread = threading.Thread(target=self.input_loop, daemon=True)
        input_thread.start()

        try:
            while not self.shutdown_flag.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.shutdown_flag.set()

        self.log("Shutting down...")
        self.executor.shutdown(wait=True)
        self.log("All tasks completed.")

if __name__ == "__main__":
    manager = TaskManager(max_workers=4)
    manager.run()
