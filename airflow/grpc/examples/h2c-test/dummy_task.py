from typing import *

import uuid
import logging
from datetime import datetime
from time import sleep

from h2c_pb2 import CommunicateMsg
from h2c_pb2_grpc import H2CServicer

logging.basicConfig(level=logging.INFO)


class Connector(H2CServicer):
    method: Callable
    def __init__(self) -> None:
        super().__init__()

    def communicate(self, request, context):
        logging.info(f'[Connector][{datetime.now()}] Got inputs from client and execute task')
        responses = self.method(request, context)
        logging.info(f'[Connector][{datetime.now()}] Send response to server')
        return CommunicateMsg(**responses)


class DummyTaskInstance:
    def __init__(self) -> None:
        self.connector = Connector()
        self.connector.method = self._run_raw_task

    def _run_raw_task(self, request, context):
        # TODO something.
        sleep(0.5)
        return {
            'task_id': request.task_id,
            'dag_id': request.dag_id,
            'data': str(uuid.uuid4()),
        }


# class DummyHandler(DummyTaskInstance, H2CServicer):
#     def from_client(self, request, context):
#         logging.info(f'[{datetime.datetime.now()}]\n{request}')
#         responses = self._run_raw_task(request, context)
#         return CommonMsg(**responses)
# dummy_handler = DummyHandler()

dummy_ti = DummyTaskInstance()
dummy_ti_connector = dummy_ti.connector
