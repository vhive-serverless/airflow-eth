from typing import *
from concurrent import futures
import grpc

from .h2c import add_ConnectServicer_to_server
from ._remote_xcomm import RemoteXComm
from ._remote_client import RemoteClient
from ._requests import requests
from ._logger import use_rxcomm_logging


def connect_ti_function(method: Callable, args: List[Any], port: int = 8081):
    ti_xcomm = RemoteXComm(method, args)
    xcomm_server = grpc.server( futures.ThreadPoolExecutor(max_workers=1) )
    xcomm_server.add_insecure_port(f"[::]:{port}")
    add_ConnectServicer_to_server(ti_xcomm, xcomm_server)

    xcomm_server.start()
    try:
        xcomm_server.wait_for_termination()
    except KeyboardInterrupt:
        xcomm_server.stop(0)


def rxcomm_():
    ...


__all__ = [
    RemoteXComm,
    RemoteClient,
    connect_ti_function,
    requests,
    use_rxcomm_logging,
]

