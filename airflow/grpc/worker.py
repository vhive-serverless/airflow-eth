from typing import Dict, List, Callable, Any
from google.protobuf.json_format import MessageToDict
from concurrent import futures
import os
import pathlib
import logging
import json
import grpc
from grpc._server import _Server


from . import (
    Response,
    ConnectServicer, 
    add_ConnectServicer_to_server
)

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.taskinstance import TaskInstance
from airflow.models.dag import DAG


'''
    REQUEST::
        payloads:
            annotations:
                dag_id: 'str'
                task_id: 'str'
                run_id: 'str'
                map_index: 'int'
            xcoms: 'dict'

    RESPONSE::
        payloads:
            status_code: 'str'
            xcoms: 'dict'
'''


class gRPCWorker(ConnectServicer, LoggingMixin):
    ti: TaskInstance = None
    exe_fn: Callable = None
    _server: _Server = None

    def __init__(
            self, 
            ti: TaskInstance, 
            dag: DAG, 
            exe_fn: Callable, 
            args: List[Any],
            _server: _Server,
        ):
        LoggingMixin.__init__(self)
        ConnectServicer.__init__(self)
        self.ti = ti
        self.dag = dag
        self.exe_fn = exe_fn
        self.args = args
        self._server = _server

    def communicate(self, request, context):
        # Parse or Unpack messages from 'gRPCScheduler'.
        request: Dict = MessageToDict(request)
        logging.info(f"Received from airflow's scheduler before eval: {request}")
        request: Dict = eval(request["payloads"])
        logging.info(f"Received from airflow's scheduler after eval: {request}")

        # Create xcomm for communication between gRPC and TaskInstance.
        # In this example, we use ...
        annotations = request["annotations"]
        base_path = pathlib.Path('/home/airflow') / annotations["dag_id"] / annotations["task_id"] / annotations["run_id"] / str(annotations["map_index"])
        os.makedirs(base_path, exist_ok=True)
        input_path = base_path / "input"
        with open(input_path, "w") as f:
            json.dump(request["xcoms"], f)
        airflow_output_path = f'{base_path}/output'
        try:
            os.remove(airflow_output_path)
        except FileNotFoundError:
            pass

        # Run TaskInstance
        self.args.execution_date_or_run_id = request["args"][5]
        self.exe_fn(self.args, self.dag, self.ti)

        # Retrieve xcomm outputs to 'gRPCScheduler'.
        try:
            with open(airflow_output_path, 'r') as f:
                xcoms = [json.loads(line) for line in f.readlines()]
        except FileNotFoundError:
            xcoms = []
        payloads = str({"xcoms": xcoms})
        return Response(payloads=payloads)

    def start(self):
        logging.info("Start to open gRPC-Server!")
        self._server.start()
        self._server.wait_for_termination()

    @staticmethod
    def serve(
        ti: TaskInstance, 
        dag: DAG, 
        exe_fn: Callable, 
        args: List[Any], 
        port: int = 8082, 
        max_workers: int = 1
    ) -> 'gRPCWorker':
        logging.info("Generate gRPC-Server!")
        _server = grpc.server( futures.ThreadPoolExecutor(max_workers=max_workers) )
        _server.add_insecure_port(f"[::]:{port}")

        logging.info("Generate Worker!")
        _grpc_worker = gRPCWorker( ti=ti, dag=dag, exe_fn=exe_fn, args=args, _server=_server )

        logging.info("Attach Worker to gRPC-Server!")
        add_ConnectServicer_to_server( _grpc_worker, _server)
        return _grpc_worker
