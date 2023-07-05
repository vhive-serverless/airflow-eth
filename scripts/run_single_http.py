import json
import requests
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def producer():
    r = requests.post("http://airflow-worker-producer.airflow.192.168.1.240.sslip.io/run_task_instance", json={"args": ['airflow', 'tasks', 'run', 'benchmark_w1_d2', 'extract', 'manual__2023-07-03T16:18:11.456156+00:00', '--local', '--subdir', 'DAGS_FOLDER/benchmark_w1_d2.py'], "xcoms": [], "annotations": {'dag_id': 'benchmark_w1_d2', 'task_id': 'extract', 'try_number': 1, 'run_id': 'manual__2023-07-03T16:18:11.456156+00:00', 'map_index': -1}})
    return r
    
def consumer(input):
    r = requests.post("http://airflow-worker-producer.airflow.192.168.1.240.sslip.io/run_task_instance", json={"args": ['airflow', 'tasks', 'run', 'benchmark_w1_d2', 'extract', 'manual__2023-07-03T16:18:11.456156+00:00', '--local', '--subdir', 'DAGS_FOLDER/benchmark_w1_d2.py'], "xcoms": [input], "annotations": {'dag_id': 'benchmark_w1_d2', 'task_id': 'extract', 'try_number': 1, 'run_id': 'manual__2023-07-03T16:18:11.456156+00:00', 'map_index': -1}})
    return r
    
def main():
    extract_response = producer()
    logging.info(f'extract_response: {extract_response.json()}')
    data = extract_response.json()
    dosum_response = consumer('10')
    logging.info(f'extract_response: {dosum_response.json()}')

    
    
if __name__ == "__main__":
    main()