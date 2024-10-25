import requests
import time

HOST = "http://node11:4978"


def get_running_flink_job():
    flink_url = HOST + "/jobs"
    response = requests.get(flink_url)
    if response.status_code == 200:
        jobs = response.json()["jobs"]
        for job in jobs:
            return job  # assume only one job
    return None


def get_task_id(job_id, task_name_contains):
    flink_url = HOST + f"/jobs/{job_id}"
    response = requests.get(flink_url)
    if response.status_code == 200:
        vertices = response.json()["vertices"]
        for vertex in vertices:
            if task_name_contains in vertex["name"]:
                return vertex["id"]
    return None


def get_taskmanagers(job_id, task_id):
    flink_url = HOST + f"/jobs/{job_id}/vertices/{task_id}"
    response = requests.get(flink_url)
    if response.status_code == 200:
        return [subtask["taskmanager-id"] for subtask in response.json()["subtasks"]]
    return None


if __name__ == "__main__":
    # Get the running Flink job
    running_job = get_running_flink_job()
    tries = 0
    while not running_job:
        time.sleep(5)
        running_job = get_running_flink_job()
        tries += 1
        if tries > 12:
            print("check-flink-task-manager.py: No running Flink job found")
            exit(0)

    print(f"Found running Flink job: {running_job['id']}")
    taskmanagers = get_taskmanagers(
        running_job["id"], get_task_id(running_job["id"], "partition")
    )
    print(f"check-flink-task-manager.py: taskmanagers: {taskmanagers}")
