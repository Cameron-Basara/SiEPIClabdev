from multiprocessing import Process, Manager
from mainProcess import MainProcess
from queryProcess import QueryProcess
from sharedIPC import create_ICP
import os
from time import sleep

# helper functions
from process_utils import kill_python_processes
import sys

def run_query_process(query_c, heartbeat_q, shutdown_flag):
    print(f"[QueryProcess] Starting with PID {os.getpid()}")
    qp = QueryProcess(query_c, heartbeat_q, shutdown_flag)
    qp.start()

def run_main_process(main_c, heartbeat_q, shutdown_flag, N=100):
    print(f"[MainProcess] Starting with PID {os.getpid()}")
    mp = MainProcess(main_c, heartbeat_q, shutdown_flag, N)
    mp.run_main()

if __name__ == "__main__":
    # clean zombie subprocesses
    if "--clean" in sys.argv:
        kill_python_processes(filter_substring="python")

    main_c, query_c, heartbeat_q = create_ICP()

    # Run with a manager allow for toggling of shutdown events
    with Manager() as manager:
        # Shutdown event
        shutdown_flag = manager.Event()

        # Define processes
        query_proc = Process(
                target=run_query_process,
                args=(query_c, heartbeat_q, shutdown_flag)
            )
        main_proc = Process(
            target=run_main_process,
            args=(main_c, heartbeat_q, shutdown_flag)
        )

        query_proc.start()
        main_proc.start()

        try:
            query_proc.join()
            main_proc.join()
        except KeyboardInterrupt:
            print("[Runner] Ctrl+C detected. Signaling shutdown...")
            shutdown_flag.set()

            # Give them time to shut down gracefully
            sleep(2)

            if query_proc.is_alive():
                print("[Runner] Force-killing query process")
                query_proc.terminate()
            if main_proc.is_alive():
                print("[Runner] Force-killing main process")
                main_proc.terminate()

            query_proc.join()
            main_proc.join()

        print("[Runner] Shutdown complete.")
