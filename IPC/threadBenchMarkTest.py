import subprocess
import matplotlib.pyplot as plt
import csv
import time
import os

# Configuration
NUM_WORKERS_LIST = [2, 4, 8]
NUM_TASKS_LIST = [1000, 10000, 100000]
TRIALS = 1
SCRIPT_PATH = "benchmarkRunner.py"
CSV_LOG = "scaling_results.csv"

def run_trial(num_workers, num_tasks):
    cmd = ["python", SCRIPT_PATH, str(num_workers), str(num_tasks)]
    start = time.perf_counter()
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    elapsed = time.perf_counter() - start

    output = proc.stdout.decode()
    print(output)

    # Parse the number of completed tasks if printed (assumes consistent format)
    lines = output.splitlines()
    tasks_completed = num_tasks  # assume perfect
    for line in lines:
        if "Completed" in line and "queries" in line:
            try:
                tasks_completed = int(line.split(" ")[1])
            except:
                pass
    return elapsed, tasks_completed

def write_csv(data):
    with open(CSV_LOG, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["NumWorkers", "NumTasks", "Trial", "TimeSec", "TasksCompleted"])
        for row in data:
            writer.writerow(row)

def plot_data(csv_file):
    import pandas as pd
    df = pd.read_csv(csv_file)

    for num_tasks in sorted(df["NumTasks"].unique()):
        sub = df[df["NumTasks"] == num_tasks]
        grouped = sub.groupby("NumWorkers")["TimeSec"].mean().reset_index()
        plt.plot(grouped["NumWorkers"], grouped["TimeSec"], label=f"{num_tasks} tasks")

    plt.xlabel("Number of Threads")
    plt.ylabel("Average Completion Time (sec)")
    plt.title("Thread Scaling Performance")
    plt.legend()
    plt.grid(True)
    plt.savefig("thread_scaling_plot.png")
    plt.show()

def main():
    results = []

    for num_workers in NUM_WORKERS_LIST:
        for num_tasks in NUM_TASKS_LIST:
            for trial in range(1, TRIALS + 1):
                print(f"Running: {num_workers} workers, {num_tasks} tasks, Trial {trial}")
                elapsed, completed = run_trial(num_workers, num_tasks)
                results.append([num_workers, num_tasks, trial, elapsed, completed])

    write_csv(results)
    plot_data(CSV_LOG)

if __name__ == "__main__":
    main()
