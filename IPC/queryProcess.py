from time import sleep
# from sharedIPC import query_c, heartbeat_q
import threading
from multiprocessing.connection import PipeConnection
# from concurrent.futures import ThreadPoolExecutor
from utils.benchmark_utils import get_memory_size
from random import randint
import heapq

class QueryTask:
    def __init__(self, priority, query_id, query_data, callback):
        self.priority = priority
        self.query_id = query_id
        self.query_data = query_data
        self.callback = callback
        self.pause_event = threading.Event()
        self.kill_event = threading.Event()
        

    def __lt__(self, other):
        return self.priority < other.priority  # Min-heap

    def run(self):
        self.callback(self.query_data, self.pause_event, self.kill_event)

class QueryProcess:
    """
    A process that manages a thread pool to handle incoming prioritized query tasks.

    Args:
        query_c (multiprocessing.Connection): IPC pipe to communicate with the main process.
        heartbeat_q (multiprocessing.Queue): Queue to publish heartbeat signals.
        shutdown_flag (multiprocessing.Event): Shared flag to signal shutdown.
        num_workers (int): Number of worker threads to spawn (default = 4).

    Methods:
        query_worker(data, pause_event, kill_event):
            Simulates execution of a query and returns a result to the main process.

        heartbeat_loop():
            Publishes "Alive" signal to the heartbeat queue every second.

        query_loop():
            Waits for incoming queries and adds them to the task queue based on priority.

        worker_loop():
            Continuously checks for available tasks and runs them using QueryTask.run().

        start():
            Starts all internal threads and blocks until shutdown_flag is set.
    """
    def __init__(self, query_c, heartbeat_q, shutdown_flag, num_workers=4):
        self.query_c = query_c
        self.heartbeat_q = heartbeat_q
        self.task_queue = [] 
        self.queue_lock = threading.Lock() 
        self.sending_lock = threading.Lock()
        self.max_workers = num_workers # default=4 Quad-core cpu
        self.workers = []
        self.shutdown_flag = shutdown_flag

        # Same as main
        if isinstance(query_c, PipeConnection):
            self._send = query_c.send
            self._recv = lambda timeout=1: query_c.recv() if query_c.poll(timeout) else None
        elif hasattr(query_c, "put") and hasattr(query_c, "get"):
            self._send = query_c.put
            self._recv = lambda timeout=1: query_c.get(timeout=timeout)
        else:
            raise TypeError(f"Unsupported query_c type: {type(query_c)}")

        for _ in range(self.max_workers):
            t = threading.Thread(target=self.worker_loop, daemon=True)
            t.start()
            self.workers.append(t)

    def query_worker(self, data, pause_event, kill_event):
        print(f"[QueryWorker] Executing: {data}")

        # Check events
        if kill_event.is_set():
            return {"id": data["id"], "cmd": data["cmd"], "value": None, "status": "killed"}
        while pause_event.is_set():
            print(f"Worker {data["id"]} is paused")
            sleep(0.1)

        result = {"id": data["id"], "cmd": data["cmd"], "value": randint(0, 10), "status": "complete"}
        
        sleep(result["value"] / 100)

        with self.sending_lock:
            self._send(result)
    
    def heartbeat_loop(self):
        while not self.shutdown_flag.is_set():
            # self.heartbeat_q.put("Alive")
            sleep(1)

    def query_loop(self):
        while not self.shutdown_flag.is_set():
            if self.query_c.poll(timeout=0.5):
                query = self.query_c.recv()

                priority = query.get("priority", 5) # pass
                task = QueryTask(
                    priority=priority,
                    query_id=query["id"],
                    query_data=query,
                    callback=self.query_worker
                )

                with self.queue_lock:
                    heapq.heappush(self.task_queue, task)
    
    def worker_loop(self):
        while not self.shutdown_flag.is_set():
            with self.queue_lock:
                if self.task_queue:
                    task = heapq.heappop(self.task_queue)
                else:
                    task = None

            if task:
                task.run()
            else:
                sleep(0.1)
    
    def recv_data(self):
        try:
            if self.query_c.poll(timeout=1):
                return self._recv()
        except:
            return None

    def test_recv_loop(self):
        while not self.shutdown_flag.is_set():
            data = self.recv_data()
            if data is not None:
                print(f"[Query] Received {type(data).__name__}, size: {get_memory_size(data)} bytes")

    def start(self):
        print("[QueryProcess]: Starting")

        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.query_loop, daemon=True).start()

        try:
            while not self.shutdown_flag.is_set():
                sleep(0.5)
        except KeyboardInterrupt:
            print("[QueryProcess]: KeyboardInterrupt caught : shutting down")
        finally:
            print("[QueryProcess]: Shutdown signal received")

    # Zombie methods
    # def query_loop(self):
    #     while self.running:
    #         if self.query_c.poll(timeout=3):  
    #             query = self.query_c.recv()
    #             threading.Thread(target=self.query_worker, args=(query,), daemon=True).start()

    # def query_loop(self):
    #     while self.running:
    #         self.executor = ThreadPoolExecutor()
    #         if self.query_c.poll(timeout=1):
    #             query = self.query_c.recv()
    #             self.executor.submit(self.query_worker, query)