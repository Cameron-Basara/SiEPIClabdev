from time import sleep, time
from random import randint
import threading
# from sharedIPC import main_c, heartbeat_q
from utils.benchmark_utils import measure_transfer_time
from multiprocessing.connection import PipeConnection
import queue
import numpy as np
import os

class MainProcess:
    def __init__(self, main_c, heartbeat_q, shutdown_flag, N):
        self.main_c = main_c
        self.heartbeat_q = heartbeat_q
        self.query_id = 1
        self.N = N
        self.shutdown_flag = shutdown_flag

        # Setup communication based on IPC type
        if isinstance(main_c, PipeConnection):  # Pipe
            self._send = main_c.send
            self._recv = lambda timeout=1: main_c.recv() if main_c.poll(timeout) else None
        elif hasattr(main_c, "put") and hasattr(main_c, "get"):  # Queue or Manager
            self._send = main_c.put
            self._recv = lambda timeout=1: main_c.get(timeout=timeout)
        else:
            raise TypeError(f"Unsupported main_c type: {type(main_c)}")

    """ Send information """
    def send_data(self, data):
        self._send(data) # send arb data to QueryProcess

    def send_query(self, id: int, cmd: str):
        query = {"id": f'{id}.1', "cmd": cmd, "priority" : randint(1,10)}
        print(f"[Main] Sending query: \n{query} ") 
        self.main_c.send(query)

    def send_numpy(self, shape=(16, 128, 64), dtype=np.float32):
        arr = np.random.rand(*shape).astype(dtype) # 1MB float32 arr
        return self.test_transfer(arr)

    def send_dict(self):
        data = {
            "id": f"{self.query_id}.test",
            "cmd": "vectorized_op",
            "values": list(np.random.randint(0, 255, 1000))
        }
        return self.test_transfer(data)

    def send_bytes(self, length=2048):
        data = os.urandom(length)
        return self.test_transfer(data) 
   
    """ Recv information """
    def receive_results(self):
        if self.main_c.poll(timeout=5):
            result = self.main_c.recv()
            print("[Main] Received result:", result)
    
    def recv_data(self):
        try:
            return self._recv(timeout=5)
        except (queue.Empty, EOFError):
            return None

    """ Test """
    def test_transfer(self, data, label=""):
        print(f"[Main] Testing transfer: {type(data).__name__}")
        result = measure_transfer_time(
            send_func=self.send_data,
            recv_func=self.recv_data,
            data=data,
            label=label
        )
        print(f"[Main] Transfer result: {result}")
        return result["transfer_time_sec"], result["data_size"]


    def heartbeat(self):
        try:
            hb = self.heartbeat_q.get(timeout=3)
            print("[Watchdog] Heartbeat OK:", hb)
        except:
            print("[Watchdog] No heartbeat. Query process might be dead.")

    """ Runners """
    def run_main(self):
        try:
            while not self.shutdown_flag.is_set():
                cmd = f"q{self.query_id}"
                self.send_query(self.query_id, cmd)
                self.receive_results()
                self.heartbeat()
                self.query_id += 1
                sleep(2)
        except KeyboardInterrupt:
            print("[MainProcess]: KeyboardInterrupt caught : shutting down")
        finally:
            print("[MainProcess]: Shutdown signal received")
    
    def run_main_100(self):
        t0 = time()

        for i in range(1, self.N + 1):
            cmd = f"q{i}"
            self.send_query(i, cmd)

        responses = 0
        while responses < self.N:
            if self.main_c.poll(timeout=1):
                result = self.main_c.recv()
                responses += 1

        t1 = time()
        print(f"[MainProcess] Completed {self.N} queries in {t1 - t0:.2f} seconds")

    def run_ipc_testers(self):
        try:
            while not self.shutdown_flag.is_set():
                cmd = f"q{self.query_id}"
                self.send_query(self.query_id, cmd)
                # self.heartbeat()
                result = self.recv_data()
                print("[Main] Received result:", result)
                self.query_id += 1
                sleep(2)
                
        except KeyboardInterrupt:
            print("[MainProcess] KeyboardInterrupt caught: shutting down")

        finally:
            print("[MainProcess] Shutdown signal received")