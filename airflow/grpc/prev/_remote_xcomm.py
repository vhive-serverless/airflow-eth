from typing import *
from google.protobuf.json_format import MessageToDict
import os
import pathlib
import json

from ._logger import _BaseLogger
from .h2c import Response, ConnectServicer 


class _BaseRemoteXComm(ConnectServicer, _BaseLogger):
    def __init__(
        self, 
        _run_task_by_selected_method: Callable,
        args: List[Any]
    ) -> None:
        _BaseLogger.__init__(self, self.__class__.__name__)
        ConnectServicer.__init__(self)
        self._run_task_by_selected_method = _run_task_by_selected_method
        self.args = args[0]
        self.dag = args[1]
        self.ti = args[2]

    def communicate(self, request, context):
        self._request = self._unpack(request)
        
        # setup xcom input and output
        annotations = self._request["annotations"]
        base_path = pathlib.Path('/home/airflow') / annotations["dag_id"] / annotations["task_id"] / annotations["run_id"] / str(annotations["map_index"])
        os.makedirs(base_path, exist_ok=True)
        input_path = base_path / "input"
        with open(input_path, "w") as f:
            json.dump(self._request["xcoms"], f)
        airflow_output_path = f'{base_path}/output'

        # Run task
        self.info(self.args)
        self.info(self._request["args"][5])
        self.args.execution_date_or_run_id = self._request["args"][5]
        self._run_task_by_selected_method(self.args, self.dag, self.ti)

        # retrieve xcom outputs
        try:
            with open(airflow_output_path, 'r') as f:
                xcoms = [json.loads(line) for line in f.readlines()]
        except FileNotFoundError:
            xcoms = []
        return Response(**self._pack({"xcoms": xcoms}))

    def _unpack(self, request):
        request = MessageToDict(request)
        request = eval(request["payloads"])
        self.info(f"Unpacking... {request}")
        return request['payloads']

    def _pack(self, response):
        self.info(f"Packing... {response}")
        return { "payloads": str(response) }

    def get(self):
        return self._request["payloads"]

    def push(self, data):
        self._response = self._pack(data)


class RemoteXComm(_BaseRemoteXComm):
    def __init__(self, ti, method: str) -> None:
        super().__init__(ti, method)
        self.info(f"Connect to task instance's method ({method})")

