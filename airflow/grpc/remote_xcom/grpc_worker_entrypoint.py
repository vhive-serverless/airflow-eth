import json
from typing import *
from concurrent import futures
import os
import pathlib
import grpc
import pickle
import argparse
import logging
import subprocess

from airflow.utils.log.logging_mixin import LoggingMixin
from protos import remote_xcom_pb2, remote_xcom_pb2_grpc

log = logging.getLogger(__name__)

class InvokeWorker(remote_xcom_pb2_grpc.TaskRunServicer, LoggingMixin):
    def HandleTask(self, request, context):
        log.info(f"Received job !")
        args = request.args
        annotations =  pickle.loads(request.annotations)
        # Command below can be safely removed; deserialized for logging purpose
        xcoms = pickle.loads(request.xcoms)
        log.info(f"args: {args}, annotations: {annotations}, xcoms: {xcoms}")
        
        base_path = pathlib.Path('/home/airflow') / annotations["dag_id"] / annotations["task_id"] / annotations["run_id"] / str(annotations["map_index"])
        os.makedirs(base_path, exist_ok=True)
        input_path = base_path / "input"
        with open(input_path, "wb") as f:
            f.write(request.xcoms)
        
        airflow_output_path = f"{base_path}/output"
        try:
            os.remove(airflow_output_path)
        except FileNotFoundError:
            pass
        
        with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as p:
            for line in p.stdout:
                log.info(f"exitcode: {p.returncode}; stdout: {line}; stderr: {p.stderr}")
        
        # find a way not to deserialize and serialize again just to put data inside a list
        try:
            with open(airflow_output_path, 'rb') as f:
                xcoms = [pickle.load(f)]
        except FileNotFoundError:
            xcoms = []
        response = pickle.dumps({"xcoms": xcoms})
        log.info(f"response: {xcoms}")
        return remote_xcom_pb2.task_reply(xcoms = response)


def serve(port: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    remote_xcom_pb2_grpc.add_TaskRunServicer_to_server(InvokeWorker(),server)
    server.add_insecure_port(f'[::]:{port}')
    log.info(f"start worker server at port [{port}]")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=8081)
    args = parser.parse_args()
    serve(args.port)
