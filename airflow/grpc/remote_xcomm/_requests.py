from typing import *
import grpc
import logging
from google.protobuf.json_format import MessageToDict

from .h2c import Call, ConnectStub


def requests(url: str, data: Any, port: int = 80):
    def _unpack(data):
        logging.info("[str] >", data)
        data = MessageToDict(data)
        logging.info("[dict] >", data)
        data = eval(data['payloads'])
        logging.info("[eval(dict)] >", data)
        return data

    def _pack(data):
        return Call( payloads=str(data) )

    def _get_target(url: str, port: int):
        prefix = 'http://'
        url = url[len(prefix):] if url.startswith(prefix) else url
        return f'{url}:{port}'

    url = _get_target(url=url, port=port)
    logging.info(url)
    with grpc.insecure_channel(url) as channel:
        response = ConnectStub(channel=channel).communicate( _pack(data) )
    return _unpack(response)

