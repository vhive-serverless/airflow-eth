TOOL=grpc_tools.protoc
NAME=$1
echo ${NAME}
python -m ${TOOL} ${NAME}.proto -I=protos --python_out=. --grpc_python_out=.

if test -f "${NAME}_server.py"; then
    echo "File exist!"
else
    echo "File is not exist!"
    touch "${NAME}_server.py"
fi
