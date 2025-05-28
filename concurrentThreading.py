import threading
import time

def crawl(link, delay=1):
    print("Crawl is executing 1")
    time.sleep(delay)
    print("Crawl is executing 2")
    time.sleep(delay)
    print("Crawl is executing 3")
    time.sleep(delay)
    print("Crawl is executing 4")

def drawl(link, delay=1):
    print("Drawl is executing 1")
    time.sleep(delay)
    print("Drawl is executing 2")
    time.sleep(delay)
    print("Drawl is executing 3")
    time.sleep(delay)
    print("Drawl is executing 4")


links = [
    "https://python.org",
    "https://docs.python.org",
    "https://peps.python.org",
]

# Start threads for each link
threads = []
for link in links:
    # Using `args` to pass positional arguments and `kwargs` for keyword arguments
    t = threading.Thread(target=crawl, args=(link,), kwargs={"delay": 2})
    n = threading.Thread(target=drawl, args=(link, ), kwargs={"delay": 1})
    threads.append(t)
    threads.append(n)

# Start each thread
for t in threads:
    t.start()

# Wait for all threads to finish
for t in threads:
    t.join()