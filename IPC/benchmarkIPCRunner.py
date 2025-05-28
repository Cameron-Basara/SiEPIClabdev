import sys
from time import sleep
from multiprocessing import Process, Manager
from mainProcess import MainProcess
from queryProcess import QueryProcess
from sharedIPC import create_IPC
from utils.benchmark_utils import get_memory_size
from utils.process_utils import kill_python_processes
import matplotlib.pyplot as plt
import pandas as pd

IPC_MODES = ["pipe", "queue", "manager"]
DATA_TESTS = ["send_dict", "send_numpy", "send_bytes"]

def run_query_process(query_c, heartbeat_q, shutdown_flag):
    qp = QueryProcess(query_c, heartbeat_q, shutdown_flag)
    qp.test_recv_loop()

def run_main_process(main_c, heartbeat_q, shutdown_flag, N, ipc_mode, result_list):
    mp = MainProcess(main_c, heartbeat_q, shutdown_flag, N)
    results = []
    for test in DATA_TESTS:
        if hasattr(mp, test):
            result = getattr(mp, test)()
            if result:
                if isinstance(result, dict):
                    time_taken = result.get("transfer_time_sec", 0)
                    size = result.get("data_size", 0)
                else:
                    time_taken, size = result
                results.append((test.replace("send_", ""), time_taken, size))
    result_list.append((ipc_mode, results))

def visualize_results(results):
    all_data = []
    for ipc_mode, entries in results:
        for dtype, t, size in entries:
            all_data.append({
                "IPC Mode": ipc_mode,
                "Data Type": dtype,
                "Time (s)": t,
                "Size (bytes)": size
            })

    df = pd.DataFrame(all_data)

    fig, ax = plt.subplots()
    df.pivot(index="Data Type", columns="IPC Mode", values="Time (s)").plot(kind="bar", ax=ax)
    plt.title("IPC Performance (Transfer Time by Data Type and Mode)")
    plt.ylabel("Transfer Time (s)")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    if "--clean" in sys.argv:
        kill_python_processes(filter_substring="python")

    manager = Manager()
    shutdown_flag = manager.Event()
    all_results = manager.list()

    for mode in IPC_MODES:
        print(f"\n[Runner] Testing IPC Mode: {mode.upper()}")
        main_c, query_c, heartbeat_q = create_IPC(mode)

        query_proc = Process(target=run_query_process, args=(query_c, heartbeat_q, shutdown_flag))
        main_proc = Process(target=run_main_process, args=(main_c, heartbeat_q, shutdown_flag, 1, mode, all_results))

        query_proc.start()
        main_proc.start()

        main_proc.join(timeout=10)
        query_proc.terminate()
        query_proc.join(timeout=5)

        print(f"[Runner] Completed mode: {mode}\n")
        shutdown_flag.set()
        sleep(1)  # Graceful cleanup

    visualize_results(all_results)
    print("[Runner] All benchmarks completed and visualized.")
