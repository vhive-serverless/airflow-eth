from typing import List, Dict, Any
from google.protobuf.json_format import MessageToDict
import grpc
import logging

from . import Call, ConnectStub

from airflow.utils.log.logging_mixin import LoggingMixin


class Connector(ConnectStub, LoggingMixin):
    def __init__(
        self, 
        task_id: str,
        worker_url: str,
        port: int = 80,
    ):
        worker_url = self.get_worker_url(worker_url, port)
        channel = grpc.insecure_channel(worker_url)
        LoggingMixin.__init__(self)
        ConnectStub.__init__(self, channel)

        self.url = worker_url
        self.task_id = task_id

    def __repr__(self) -> str:
        _repr = f"[TI ({self.status}) > {self.task_id}] URL: {self.url}."
        return _repr

    def send(self, payloads: Dict[Any, Any]) -> Dict[Any, Any]:
        # Encoding payloads
        payloads = str(payloads)
        # Send payloads to the worker
        response = self.communicate( Call(payloads=payloads) )
        response = MessageToDict(response)
        response = eval(response["payloads"])
        response = response["payloads"]
        return response

    @property
    def status(self):
        return "Active"

    @staticmethod
    def get_worker_url(worker_url: str, port: int = 80):
        logging.info(f"Sending Message to endopoint(1): {worker_url}")
        prefix = "http://"
        worker_url = worker_url[len(prefix):] \
            if worker_url.startswith(prefix) \
            else worker_url
        return f'{worker_url}:{port}'
        # logging.info(f"Sending Message to endopoint(2): {worker_url}")
        # return worker_url

    @staticmethod
    def send(endpoint: str, payloads: Dict[Any, Any]):
        '''
            If you wish to use 'Connector' without definition,
            you can use this class method to send payloads/data to workers.

            Parameters::
                - endpoint (str-type): 'http://[url]:[port]',
                - payloads (dict-type): { key0:value0, key1:value1, ..., }

            Returns::
                - response (dict-type): { key0:value0, key1:value1, ..., }
        '''
        # endpoint += ":8080"
        response = None
        endpoint = Connector.get_worker_url(endpoint)
        logging.info(f"Sending Message to endopoint(3): {endpoint}")
        logging.info(f"Sending Message via 'Connector.send()': {payloads}")
        # try:
        #     with grpc.insecure_channel(endpoint) as channel:
        #         payloads = str(payloads)
        #         logging.info(f"Try to send Message to Worker(1), payloads={payloads}")
        #         stub = ConnectStub(channel)
        #         logging.info(f"Try to send Message to Worker(2), payloads={payloads}")
        #         response = stub.communicate( Call( payloads=payloads ) )
        #         logging.info(f"Response Message(1): {response}")
        #         response = MessageToDict(response)
        #         logging.info(f"Response Message(2): {response}")
        #         response = eval(response['payloads'])
        #         logging.info(f"Response Message(3): {response}")
        #         # response = response['payloads']
        # except Exception as e:
        #     logging.error(e)
        #     response = None
        # return response

        with grpc.insecure_channel(endpoint) as channel:
            payloads = str(payloads)
            stub = ConnectStub(channel)
            try:
                response = stub.communicate( Call( payloads=payloads ) )
                logging.info(f"Response Message(1): {response}")
                response = MessageToDict(response)
                logging.info(f"Response Message(2): {response}")
                response = eval(response['payloads'])
                logging.info(f"Response Message(3): {response}")
            except Exception as e:
                logging.error(e)
            return response


class gRPCConnector(LoggingMixin):
    def __init__(
        self, 
        task_ids: List[str],
        worker_urls: List[str],
        ports: List[int],
        context=None,
    ):
        super().__init__(context)
        self.connectors: Dict[str, Connector] = {
            tid: Connector(
                task_id=tid, worker_url=url, port=port
            ) for tid, url, port in zip(task_ids, worker_urls, ports)
        }

    def __repr__(self) -> str:
        _repr = f"\n[{self.__class__.__name__}]"
        for tid in self.connectors:
            _repr += f"\n\t{self.connectors[tid]}"
        return _repr

    def __getattr__(self, __taskId: str) -> Connector:
        connector = self.connectors[__taskId]
        return connector

    def send(self, task_id: str, paylods: Dict[Any, Any]):
        return self.connectors[task_id].send(payloads=paylods)

