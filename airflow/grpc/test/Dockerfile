FROM python:3.8-slim

RUN pip install protobuf grpcio

WORKDIR /app
COPY remote_xcomm ./remote_xcomm
COPY server.py .

ENV PORT 8081
CMD python server.py -p $PORT

