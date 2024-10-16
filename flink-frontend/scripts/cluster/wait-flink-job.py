import sys
import requests
import time
import os

nodes = ['node10', 'node11', 'node12', 'node13', 'node14', 'node15', 'node182', 'node21', 'node22', 'node23']
HOST = 'http://node11:4978'
vertex_name = 'Source: source input'
vertex_max_write_records = 1000000000
max_wait_hours = 24 * 7

def get_running_flink_job():
    flink_url = HOST + '/jobs'
    response = requests.get(flink_url)
    if response.status_code == 200:
        jobs = response.json()['jobs']
        for job in jobs:
            if job["status"] == "RUNNING":
                return job
    return None

def wait_for_task_completion(job_id):
    flink_url = HOST + f"/jobs/{job_id}"
    start_time = time.time()
    while True:
        if time.time() - start_time > max_wait_hours * 3600:
            return 'TIMEOUT'
        response = requests.get(flink_url)
        if response.status_code == 200:
            vertices = response.json()['vertices']
            statuses = []
            for vertex in vertices:
                if vertex["status"] == "FINISHED" or vertex["status"] == "FAILED" or vertex['status'] == 'CANCELED':
                    statuses.append(vertex["status"])
                if vertex['name'] == vertex_name and vertex['metrics']['write-records'] >= vertex_max_write_records:
                    statuses.append('FINISHED')
            if statuses:
                if 'FAILED' in statuses:
                    return 'FAILED'
                elif 'CANCELED' in statuses:
                    return 'CANCELED'
                elif 'FINISHED' in statuses:
                    return 'FINISHED'
        else:
            print('Failed to get job vertices')
        time.sleep(30)

if __name__ == '__main__':
    folder = sys.argv[1]

    # Get the running Flink job
    running_job = get_running_flink_job()
    if not running_job:
        print('Could not find a running Flink job')
        exit(1)

    print(f"Found running Flink job: {running_job['id']}")
    print("Waiting for a task to finish...")
    status = wait_for_task_completion(running_job['id'])
    print(f"A task has {status}!")

    if status == "FAILED":
        exit(1)
    exit(0)
