import os
import psutil
import time
import matplotlib.pyplot as plt
import numpy as np
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import matplotlib.pyplot as plt
import pandas as pd

def plot_ipc_performance_bar(result_dict):
    """
    Plots a grouped bar chart showing IPC transfer times across different data types and modes.
    """
    records = []
    for ipc_mode, entries in result_dict.items():
        for dtype, t, s in entries:
            records.append({
                "IPC Mode": ipc_mode,
                "Data Type": dtype,
                "Transfer Time (s)": t,
                "Data Size (Bytes)": s
            })

    df = pd.DataFrame(records)

    # Create a grouped bar plot
    fig, ax = plt.subplots(figsize=(10, 6))

    width = 0.2
    positions = range(len(df["IPC Mode"].unique()))
    x_labels = list(df["IPC Mode"].unique())

    for i, dtype in enumerate(df["Data Type"].unique()):
        subset = df[df["Data Type"] == dtype]
        x = [p + i * width for p in positions]
        ax.bar(x, subset["Transfer Time (s)"], width=width, label=dtype)

    ax.set_xticks([p + width for p in positions])
    ax.set_xticklabels(x_labels)
    ax.set_ylabel("Transfer Time (s)")
    ax.set_title("IPC Transfer Time by Mode and Data Type")
    ax.legend()
    ax.grid(True)
    plt.tight_layout()
    plt.show()

def plot_ipc_performance(result_dict):
    import pandas as pd
    import matplotlib.pyplot as plt

    rows = []
    for mode, records in result_dict.items():
        for name, time_sec, size in records:
            rows.append({
                "ipc_mode": mode,
                "data_type": name,
                "time_sec": time_sec,
                "data_size_bytes": size,
                "memory_mb": size / (1024 * 1024)  # fake placeholder if needed
            })
    df = pd.DataFrame(rows)

    for dtype in df["data_type"].unique():
        subset = df[df["data_type"] == dtype]
        plt.plot(subset["ipc_mode"], subset["time_sec"], label=f"{dtype} Time")

    plt.scatter()
    plt.ylabel("Time (s)")
    plt.xlabel("IPC Mode")
    plt.legend()
    plt.title("IPC Transfer Time by Data Type")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def measure_time(func, *args, **kwargs):
    """
    Function to measure time taken to execute a function
    """
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    return end_time - start_time, result

def measure_memory(func, *args, **kwargs):
    """
    Function to measure memory usage in MB during execution
    """
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)  # in MB
    result = func(*args, **kwargs)
    mem_after = process.memory_info().rss / (1024 * 1024)  # in MB
    return mem_after - mem_before, result

def measure_performance(func, *args, **kwargs):
    """
    Combined function to measure time and memory
    """
    t0 = time.perf_counter()
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)  # MB

    result = func(*args, **kwargs)

    t1 = time.perf_counter()
    mem_after = process.memory_info().rss / (1024 * 1024)  # MB

    return {
        "time_sec": t1 - t0,
        "memory_mb": mem_after - mem_before,
        "result": result
    }

def plot_performance(data, title="Performance Comparison"):
    """
    Function to plot performance
    """
    fig, ax1 = plt.subplots()

    ax2 = ax1.twinx()
    ipc_methods = [entry["method"] for entry in data]
    times = [entry["time_sec"] for entry in data]
    memories = [entry["memory_mb"] for entry in data]

    ax1.bar(ipc_methods, times, color='g', label='Time (s)')
    ax2.plot(ipc_methods, memories, 'b--o', label='Memory (MB)')

    ax1.set_xlabel('IPC Method')
    ax1.set_ylabel('Time (s)', color='g')
    ax2.set_ylabel('Memory (MB)', color='b')
    plt.title(title)
    fig.tight_layout()
    plt.grid(True)
    plt.show()

def measure_transfer_time(send_func, recv_func, data, label=""):
    import psutil
    import time
    import numpy as np
    import os

    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss

    start_time = time.perf_counter()
    send_func(data)
    received = recv_func()
    end_time = time.perf_counter()

    mem_after = process.memory_info().rss
    memory_used = mem_after - mem_before
    elapsed_time = end_time - start_time

    # Calculate size of data sent
    if isinstance(data, np.ndarray):
        size = data.nbytes
        success = np.array_equal(received, data)
    elif isinstance(data, (bytes, bytearray)):
        size = len(data)
        success = received == data
    else:
        import sys
        size = sys.getsizeof(data)
        success = received == data

    return {
        "label": label,
        "transfer_time_sec": elapsed_time,
        "memory_used_bytes": memory_used,
        "data_type": type(data).__name__,
        "data_shape": getattr(data, "shape", None),
        "data_size": size,
        "success": success
    }

def get_memory_size(obj):
    if isinstance(obj, np.ndarray):
        return obj.nbytes
    return sys.getsizeof(obj)

# Zombie functions

# def measure_transfer_time(send_func, recv_func, data, label=""):
#     """
#     Measures the time and memory used to send and receive data between processes.

#     Args:
#         send_func: Function that sends data.
#         recv_func: Function that receives data.
#         data: The data to send.
#         label: Optional label for logging.

#     Returns:
#         dict: Contains transfer time, memory used, and label.
#     """
#     process = psutil.Process(os.getpid())
#     mem_before = process.memory_info().rss

#     start_time = time.perf_counter()
#     send_func(data)
#     received = recv_func()
#     end_time = time.perf_counter()

#     mem_after = process.memory_info().rss
#     memory_used = mem_after - mem_before
#     elapsed_time = end_time - start_time

#     return {
#         "label": label,
#         "transfer_time_sec": elapsed_time,
#         "memory_used_bytes": memory_used,
#         "data_type": type(data).__name__,
#         "data_shape": getattr(data, "shape", None),
#         "data_size": getattr(data, "nbytes", len(str(data)) if data else 0),
#         "success": received == data if not isinstance(data, np.ndarray) else np.array_equal(data, received)
#     }

