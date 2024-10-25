import requests
from datetime import datetime
import time
import sched


HOST = "10.214.151.20"
PORT = "8081"


def get_task_managers():
    url = "http://" + HOST + ":" + PORT + "/taskmanagers"
    response = requests.get(url)
    data = response.json()
    if response.status_code == 200:
        names = [taskmanager["id"] for taskmanager in data["taskmanagers"]]
        return names
    return None


def monitor(taskmanager):
    url = "http://" + HOST + ":" + PORT + "/taskmanagers/" + taskmanager
    response = requests.get(url)
    data = response.json()
    if response.status_code == 200:
        metrics = data["metrics"]
        heap_used = metrics["heapUsed"]
        non_heap_used = metrics["nonHeapUsed"]
        direct_used = metrics["directUsed"]
        mapped_used = metrics["mappedUsed"]
        netty_shuffle_memory_used = metrics["nettyShuffleMemoryUsed"]
        return (
            heap_used,
            non_heap_used,
            direct_used,
            mapped_used,
            netty_shuffle_memory_used,
        )
    return None


def monitor_all(s):
    s.enter(10, 1, monitor_all, (s,))
    taskmanagers = sorted(get_task_managers())
    ts = datetime.now()
    print("Refresh: " + ts.strftime("%Y-%m-%d %H:%M:%S"))
    for taskmanager in taskmanagers:
        result = [str(x) for x in monitor(taskmanager)]
        print(taskmanager + " " + " ".join(result))


if __name__ == "__main__":
    s = sched.scheduler(time.time, time.sleep)
    s.enter(10, 1, monitor_all, (s,))
    s.run()
