from typing import Dict, List, Union, Callable, Any
from google.protobuf.json_format import MessageToDict
from concurrent import futures
from queue import Queue

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

class PseudoLog:
    def info(self, msg: str):
        print(str(msg))

    def error(self, msg: str):
        print(str(msg))


class WConnector_(ConnectServicer, LoggingMixin):
    ti = None
    exe_fn: Callable = None
    _server: _Server = None

    def __init__(
            self, 
            ti, 
            dag, 
            args: Any,
            target_fn: Callable, 
            port: int = 8080,
            max_workers: int = 1,
        ):
        LoggingMixin.__init__(self)
        ConnectServicer.__init__(self)
        self.ti = ti
        # self.ti.log = PseudoLog()

        self.dag = dag
        self.args = args
        self.target_fn = target_fn

        logging.info("Generate gRPC-Server!")
        self._server = grpc.server( futures.ThreadPoolExecutor(max_workers=max_workers) )
        self._server.add_insecure_port(f"[::]:{port}")

    def wait_for_termination(self):
        logging.info("Start to open gRPC-Server!")
        add_ConnectServicer_to_server( self, self._server)
        self._server.start()
        self._server.wait_for_termination()

    def communicate(self, request, context):
        request: Dict = MessageToDict(request)
        request: Dict = eval(request["payloads"])
        logging.info(f"Request from Scheduler: {request}")

        try:
            logging.info(f"ti: {self.ti}")
            logging.info(f"target: {self.target_fn}")
            logging.info(f"jobId: {self.args.job_id}")
            # Run TaskInstance
            self.target_fn(self.args, self.dag, self.ti)
        except Exception as e:
            logging.error(e)
        # with self.target_fn as f:
        #     f(self.args, self.dag, self.ti)

        xcoms = 0
        payloads = str({"status_code": 200, "xcoms": xcoms})
        return Response(payloads=payloads)


### -------- Use Decorator (if don't use it, Comment out!) -------- ###
from functools import wraps


class WConnector(ConnectServicer):
    target_fn = None
    args: Any = None
    result: Any = None

    def __init__(self, port: int = 8080, max_workers: int = 1):
        super().__init__()
        logging.info("Generate gRPC-Server!")
        self._server = grpc.server( futures.ThreadPoolExecutor(max_workers=max_workers) )
        self._server.add_insecure_port(f"[::]:{port}")

    def wait_for_termination(self):
        logging.info("Start to run gRPC Server!")
        add_ConnectServicer_to_server( self, self._server)
        self._server.start()
        self._server.wait_for_termination()

    def attach(self, _target):
        logging.info(f"Attach TaskInstance.function! {_target}")
        self.target_fn = _target
        @wraps(_target)
        def decorator(*args, **kwargs):
            return _target(*args, **kwargs)
        return decorator

    def add(self, _ti, _args):
        logging.info(f"Add TaskInstance! {_ti}")
        self.ti = _ti
        self.args = _args

    def communicate(self, request, context):
        request: Dict = MessageToDict(request)
        request: Dict = eval(request["payloads"])
        logging.info(f"Request from Scheduler: {request}")

        try:
            logging.info(f"ti: {self.ti}")
            logging.info(f"target: {self.target_fn}")
            logging.info(f"jobId: {self.args.job_id}")
            # Run TaskInstance
            self.target_fn(
                self.ti,
                mark_success=self.args.mark_success,
                job_id=self.args.job_id,
                pool=self.args.pool,
            )
        except Exception as e:
            logging.error(e)

        xcoms = 0
        payloads = str({"status_code": 200, "xcoms": xcoms})
        return Response(payloads=payloads)


wconn = WConnector(port=8083)


### -------- Use @provide_session (if don't use it, Comment out!) -------- ###
# from sqlalchemy.orm.session import Session
# from airflow.utils.session import provide_session, NEW_SESSION

# class WConnector2(ConnectServicer):
#     ti = None
#     target_fn = None
#     args: Any = None
#     result: Any = None

#     def __init__(
#         self, 
#         ti,
#         args,
#         port: int = 8080, 
#         max_workers: int = 1
#     ):
#         super().__init__()
#         logging.info("Generate gRPC-Server!")

#         self.ti = ti
#         self.args = args
#         self._server = grpc.server( futures.ThreadPoolExecutor(max_workers=max_workers) )
#         self._server.add_insecure_port(f"[::]:{port}")

#     def wait_for_termination(self):
#         logging.info("Start to run gRPC Server!")
#         add_ConnectServicer_to_server( self, self._server)
#         self._server.start()
#         self._server.wait_for_termination()

#     @provide_session
#     def communicate(self, request, context, session: Session = NEW_SESSION):
#         # Parse or Unpack messages from 'gRPCScheduler'.
#         request: Dict = MessageToDict(request)
#         request: Dict = eval(request["payloads"])
#         logging.error(f"[Is not Error!, Just for debugging!] Request from Scheduler: {request}")

#         # Create xcomm for communication between gRPC and TaskInstance.
#         # In this example, we use ...
#         # annotations = request["annotations"]
#         # base_path = pathlib.Path('/home/airflow') / annotations["dag_id"] / annotations["task_id"] / annotations["run_id"] / str(annotations["map_index"])
#         # os.makedirs(base_path, exist_ok=True)

#         # input_path = base_path / "input"
#         # with open(input_path, "w") as f:
#         #     json.dump(request["xcoms"], f)
#         # airflow_output_path = f'{base_path}/output'


#         # try:
#         #     os.remove(airflow_output_path)
#         # except FileNotFoundError:
#         #     pass

#         # Run TaskInstance
#         self.ti._run_raw_task(
#             mark_success=self.args.mark_success,
#             job_id=self.args.job_id,
#             pool=self.args.pool,
#         )

#         # Retrieve xcomm outputs to 'gRPCScheduler'.
#         # try:
#         #     with open(airflow_output_path, 'r') as f:
#         #         xcoms = [json.loads(line) for line in f.readlines()]
#         # except FileNotFoundError:
#         #     xcoms = []
#         xcoms = 0
#         payloads = str({"xcoms": xcoms})
#         return Response(payloads=payloads)

