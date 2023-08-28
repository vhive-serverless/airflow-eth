from typing import *
from concurrent import futures
import grpc
import pickle
import argparse
import logging

from airflow.grpc.remote_xcom.protobufs import remote_xcom_pb2, remote_xcom_pb2_grpc


def run(target):
    print(f"target: {target}")
    with grpc.insecure_channel(target) as channel:
        stub = remote_xcom_pb2_grpc.TaskRunStub(channel)
        
        args = ['airflow', 'tasks', '1', 'benchmark_w1_d2', 'extract', 'manual__2023-08-06T13:28:11.663263+00:00', '--local', '--subdir', 'DAGS_FOLDER/benchmark_w1_d2.py']
        annotations = {'dag_id': 'benchmark_w1_d2', 'task_id': 'extract', 'try_number': 1, 'run_id': 'manual__2023-08-06T13:28:11.663263+00:00', 'map_index': -1}
        xcoms = [1,2,3,4]
        
        job = remote_xcom_pb2.task_invoke(args = args, annotations= pickle.dumps(annotations), xcoms = pickle.dumps(xcoms))
        response = stub.HandleTask(job)
        response = pickle.loads(response.xcoms)
        print(f"response: {response}")
        
def get_target(server, port):
    prefix = 'http://'
    server = server[len(prefix):] if server.startswith(prefix) else server
    print(server)
    return f'{server}:{port}'

def invoke_task(target: str, args, annotations, xcoms):
    target = get_target(target, 80)
    with grpc.insecure_channel(target) as channel:
        stub = remote_xcom_pb2_grpc.TaskRunStub(channel)
        task = remote_xcom_pb2.task_invoke(
            args = args, 
            annotations= pickle.dumps(annotations), 
            xcoms = pickle.dumps(xcoms)
        )
        response = stub.HandleTask(task)
    return response
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--server", type=str, default="localhost")
    parser.add_argument('-p', '--port', type=int, default=8080)
    args = parser.parse_args()
    run(get_target(args.server, args.port))