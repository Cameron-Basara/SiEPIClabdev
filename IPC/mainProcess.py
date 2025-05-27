from time import sleep, time
from random import randint
import threading
# from sharedIPC import main_c, heartbeat_q

class MainProcess:
    def __init__(self, main_c, heartbeat_q, shutdown_flag, N):
        self.main_c = main_c
        self.heartbeat_q = heartbeat_q
        self.query_id = 1
        self.N = N
        self.shutdown_flag = shutdown_flag

    def send_query(self, id: int, cmd: str):
        query = {"id": f'{id}.1', "cmd": cmd, "priority" : randint(1,10)}
        # query2 = {"id": f'{id}.2', "cmd": cmd, "priority" : randint(1,10)}
        print(f"[Main] Sending query: \n{query} ") # \n{query2}")
        self.main_c.send(query)
        # self.main_c.send(query2)

    def receive_results(self):
        if self.main_c.poll(timeout=2):
            result = self.main_c.recv()
            print("[Main] Received result:", result)

    def heartbeat(self):
        try:
            hb = self.heartbeat_q.get(timeout=3)
            print("[Watchdog] Heartbeat OK:", hb)
        except:
            print("[Watchdog] No heartbeat. Query process might be dead.")

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