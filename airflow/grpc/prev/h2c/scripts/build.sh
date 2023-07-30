TOOL=grpc_tools.protoc
NAME=h2c

python -m ${TOOL} ${NAME}.proto -I=protos --python_out=. --grpc_python_out=.
