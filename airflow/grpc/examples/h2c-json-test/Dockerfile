FROM python:3.8-slim

RUN pip install protobuf grpcio

WORKDIR /app
COPY *_server.py *_pb2.py *_pb2_grpc.py dummy_task.py ./

ENV PORT 8080
CMD python *_server.py -p $PORT

