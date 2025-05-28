import threading
import heapq
import time
import queue
from typing import Callable

class TaskWrapper:
    def __init__(self, func: Callable[[str], None], priority: int, name: str):
        self.name = name
        self.priority = priority
        self.func = func

        self.killed = threading.Event()
        self.paused = threading.Event()
        self.finished = threading.Event()
        self.timestamp = time.time()
        self.thread = threading.Thread(target=self._runner, name=f"Worker: {self.name}")

    def __lt__(self, other):
        return (-self.priority, self.timestamp) < (-other.priority, other.timestamp)

    def _runner(self):
        print(f"[{self.name}] Thread started.")
        while not self.killed.is_set():
            if self.paused.is_set():
                time.sleep(0.1)
                continue
            self.func(self.name)
            self.finished.set()
            break
        print(f"[{self.name}] Thread exited.")

    def start(self):
        self.thread.start()

    def pause(self):
        print(f"[{self.name}] Paused.")
        self.paused.set()

    def resume(self):
        print(f"[{self.name}] Resumed.")
        self.paused.clear()

    def kill(self):
        print(f"[{self.name}] Killed.")
        self.killed.set()


class ThreadedTaskManager:
    def __init__(self):
        self.tasks = {}  # name → TaskWrapper
        self.queue = []
        self.lock = threading.Lock()
        self.commands = queue.Queue()
        self.shutdown = threading.Event()

    def submit_task(self, func: Callable[[str], None], priority: int, name: str):
        task = TaskWrapper(func, priority, name)
        with self.lock:
            heapq.heappush(self.queue, task)
            self.tasks[name] = task
        print(f"[Manager] Task '{name}' submitted with priority {priority}.")

    def run(self):
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while not self.shutdown.is_set():
            self._handle_commands()

            with self.lock:
                if self.queue:
                    task = heapq.heappop(self.queue)
                    if not task.killed.is_set():
                        task.start()
            time.sleep(0.1)

    def _handle_commands(self):
        while not self.commands.empty():
            cmd, name = self.commands.get()
            task = self.tasks.get(name)
            if task:
                if cmd == "pause":
                    task.pause()
                elif cmd == "resume":
                    task.resume()
                elif cmd == "cancel":
                    task.kill()
                else:
                    print(f"[Manager] Unknown command: {cmd}")


def example_task(name):
    for i in range(5):
        print(f"[{name}] Step {i}")
        time.sleep(1)


def input_loop(manager: ThreadedTaskManager):
    print("Command format: [pause|resume|cancel] <task_name>\nType 'exit' to quit.")
    while not manager.shutdown.is_set():
        try:
            user_input = input(">>> ").strip()
            if user_input.lower() in {"exit", "quit"}:
                manager.shutdown.set()
                break
            cmd, name = user_input.split()
            manager.commands.put((cmd.lower(), name))
        except ValueError:
            print("Invalid format. Example: pause task1")
        except KeyboardInterrupt:
            manager.shutdown.set()
            break


if __name__ == "__main__":
    manager = ThreadedTaskManager()
    manager.submit_task(example_task, priority=5, name="task1")
    manager.submit_task(example_task, priority=10, name="task2")
    manager.run()

    # Start user input loop
    threading.Thread(target=input_loop, args=(manager,), daemon=True).start()

    # Keep main thread alive
    try:
        while not manager.shutdown.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        manager.shutdown.set()
        print("Manager shutdown requested.")
