import psutil
import os

def kill_python_processes(filter_substring=None, exclude_self=True):
    """
    Kill Python subprocesses that match a specific filter string in the command line.

    Args:
        filter_substring (str): Only kill processes whose cmdline contains this string.
                                Useful to avoid killing unrelated Python processes.
        exclude_self (bool): Don't kill the current process.
    """
    killed = []
    current_pid = os.getpid()

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] != 'python.exe' and 'python' not in proc.info['name'].lower():
                continue

            if exclude_self and proc.info['pid'] == current_pid:
                continue

            cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''

            if filter_substring and filter_substring not in cmdline:
                continue

            print(f"[KILL] Killing PID {proc.info['pid']}: {cmdline}")
            proc.kill()
            killed.append(proc.info['pid'])

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    print(f"[KILL] Total processes killed: {len(killed)}")
    return killed
