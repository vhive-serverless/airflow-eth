from typing import *
import grpc
from google.protobuf.json_format import MessageToDict

from ._logger import _BaseLogger
from .h2c import Call, ConnectStub


class _BaseRemoteClient(ConnectStub, _BaseLogger):
    def __init__(self, url: str, port: int = 80) -> None:
        server_url = self._get_target(url, port)
        channel = grpc.insecure_channel(server_url)
        _BaseLogger.__init__(self, self.__class__.__name__)
        ConnectStub.__init__(self, channel)

    @staticmethod
    def _get_target(url: str, port: int) -> str:
        prefix = 'http://'
        url = url[len(prefix):] if url.startswith(prefix) else url
        return f'{url}:{port}'

    def request(self, request: Dict[str, str]) -> Tuple[str, Dict[str, Any]]:
        request = self._pack(request)
        response = self.communicate( request )
        response = self._unpack(response)
        return response

    def _unpack(self, response) -> Tuple[str, Dict[str, Any]]:
        response = MessageToDict(response)
        return eval(response['payloads'])

    def _pack(self, request):
        return Call( payloads=str(request) )


class RemoteClient(_BaseRemoteClient):
    def __init__(self, url: str, port: int = 80) -> None:
        super().__init__(url, port)

