import json
from typing import *
from concurrent import futures
import os
import pathlib
import grpc
import pickle
import logging
import subprocess
# from  argparse import ArgumentParser
from airflow.utils.cli import get_dag
from airflow.utils.dates import timezone
from airflow.cli import cli_parser
from airflow.cli.commands import task_command

from airflow.grpc.remote_xcom.protobufs import remote_xcom_pb2, remote_xcom_pb2_grpc

log = logging.getLogger(__name__)

class InvokeWorker(remote_xcom_pb2_grpc.TaskRunServicer):
    def __init__(self) -> None:
        log.info(f"init_start")
        super().__init__()
        self.parser = cli_parser.get_parser()
        dag_id = os.getenv("AIRFLOW_DAG_ID")
        task_id = os.getenv("AIRFLOW_TASK_ID")
        self.dag = get_dag(f"DAGS_FOLDER/{dag_id}.py", dag_id, include_examples=False)
        self.task = self.dag.get_task(task_id=task_id)
        self.ti, _ = task_command._get_ti_without_db(self.task, -1, create_if_necessary="memory")
        self.ti.init_run_context(raw=False)
        log.info(f"init_finish")
        
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
        
        args = self.parser.parse_args(args[1:])
        self.ti.dag_run.run_id = args.execution_date_or_run_id
        self.ti.dag_run.execution_date = timezone.parse(args.execution_date_or_run_id[8:])
        self.ti.run_id = args.execution_date_or_run_id
        log.info(f"parsed_args: {args}, dag: {self.dag}, task: {self.task}")
        task_command.task_run(args, dag = self.dag, task=self.task, ti = self.ti)       
        
        # find a way not to deserialize and serialize again just to put data inside a list
        try:
            with open(airflow_output_path, 'rb') as f:
                xcoms = [pickle.load(f)]
        except FileNotFoundError:
            xcoms = []
        response = pickle.dumps({"xcoms": xcoms})
        log.info(f"response: {xcoms}")
        return remote_xcom_pb2.task_reply(xcoms = response)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    remote_xcom_pb2_grpc.add_TaskRunServicer_to_server(InvokeWorker(),server)
    server.add_insecure_port(f'[::]:{8081}')
    log.info(f"start worker server at port [{8081}]")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
