import argparse
import grpc
import json
from google.protobuf.json_format import MessageToDict

from h2c_pb2 import JSONMessage
from h2c_pb2_grpc import H2CStub


def run(target):
    with grpc.insecure_channel(target) as channel:
        dict_data = {
            "user": "phw",
            "age": "27",
        }
        payloads = str(dict_data)
        stub = H2CStub(channel)

        response = stub.communicate(
            JSONMessage(
                dag_id='1', task_id='1', payloads=payloads,
            )
        )
        response = MessageToDict(response)
        print(f'[HTTP2] Response >>> {response}')


def get_target(server, port):
    prefix = 'http://'
    server = server[len(prefix):] if server.startswith(prefix) else server
    return f'{server}:{port}'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server', type=str, default='http://airflow-grpc-test-v2.airflow-grpc.192.168.1.240.sslip.io')
    parser.add_argument('-p', '--port', type=int, default=80)
    args = parser.parse_args()

    run(get_target(args.server, args.port))

