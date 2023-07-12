from typing import *

import uuid
import logging
from datetime import datetime
from time import sleep

from google.protobuf.json_format import MessageToDict

from h2c_pb2 import JSONMessage
from h2c_pb2_grpc import H2CServicer

logging.basicConfig(level=logging.INFO)


class Connector(H2CServicer):
    method: Callable
    @staticmethod
    def response_to_payloads(payloads_dict: Dict[str, Any]) -> str:
        payloads = str(payloads_dict)
        return payloads

    def _request_to_dict(self, request) -> Dict[str, Any]:
        request = MessageToDict(request)
        logging.info(f"[Connector] {request}")
        return request

    def communicate(self, request, context):
        logging.info(f'[Connector][{datetime.now()}] Got inputs from client and execute task')

        responses = self.method(
            self._request_to_dict(request), 
            context
        )

        logging.info(f'[Connector][{datetime.now()}] Send response to server')
        return JSONMessage(**responses)


class DummyTaskInstance:
    def __init__(self) -> None:
        self.connector = Connector()
        self.connector.method = self._run_raw_task

    def _run_raw_task(self, request, context):
        # TODO something.
        sleep(0.5)
        payloads = self.connector.response_to_payloads(
            {
                "output": str(uuid.uuid4()),
                "output_key": "hello_world"
            }
        )
        return {
            'task_id': request["taskId"],
            'dag_id': request["dagId"],
            'payloads': payloads,
        }


dummy_ti = DummyTaskInstance()
dummy_ti_connector = dummy_ti.connector
