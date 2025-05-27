from multiprocessing import Process, Manager
from mainProcess import MainProcess
from queryProcess import QueryProcess
from sharedIPC import create_ICP
import os
import sys
import time


def run_query_process(query_c, heartbeat_q, shutdown_flag, num_workers):
    print(f"[QueryProcess] Starting with PID {os.getpid()}, Workers: {num_workers}")
    qp = QueryProcess(query_c, heartbeat_q, shutdown_flag, num_workers=num_workers)
    qp.start()


def run_main_process(main_c, heartbeat_q, shutdown_flag, N=100):
    print(f"[MainProcess] Starting with PID {os.getpid()}")
    mp = MainProcess(main_c, heartbeat_q, shutdown_flag, N)
    mp.run_main_100()


if __name__ == "__main__":
    # Usage: python benchmark_runner.py <num_workers> [--clean]
    if len(sys.argv) < 2:
        print("Usage: python benchmark_runner.py <num_workers> <num_cmds> [--clean]")
        sys.exit(1)

    num_workers = int(sys.argv[1])
    num_cmds = int(sys.argv[2])

    from process_utils import kill_python_processes
    if "--clean" in sys.argv:
        kill_python_processes(filter_substring="python")

    main_c, query_c, heartbeat_q = create_ICP()

    with Manager() as manager:
        shutdown_flag = manager.Event()

        query_proc = Process(
            target=run_query_process,
            args=(query_c, heartbeat_q, shutdown_flag, num_workers)
        )

        main_proc = Process(
            target=run_main_process,
            args=(main_c, heartbeat_q, shutdown_flag, num_cmds)
        )

        query_proc.start()
        main_proc.start()

        start_time = time.perf_counter()

        try:
            main_proc.join()  # Wait for the main to finish sending/receiving queries

            # Now that main process is done, shut down the query side too
            shutdown_flag.set()

            query_proc.join(timeout=5)
            if query_proc.is_alive():
                print("[BenchmarkRunner] Forcing query process to terminate")
                query_proc.terminate()
                query_proc.join()

        except KeyboardInterrupt:
            print("[BenchmarkRunner] Ctrl+C detected. Shutting down.")
            shutdown_flag.set()
            query_proc.terminate()
            main_proc.terminate()
            query_proc.join()
            main_proc.join()

       
        
