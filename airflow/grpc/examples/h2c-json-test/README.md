## Scripts
```
$ ./scripts/build.sh <gRPC-proto-nam> # ./scripts/build.sh h2c
$ ./scripts/deploy.sh   # Deploy pod on the Knative clusters
$ ./scripts/delete.sh   # Delete pod
```

## Experiments
```
$ echo $(kubectl get ksvc -n airflow-grpc airflow-grpc-test-v2 -o jsonpath='{.status.url}')
http://airflow-grpc-test-v2.airflow-grpc.192.168.1.240.sslip.io
$ python h2c_client.py -s http://airflow-grpc-test-v2.airflow-grpc.192.168.1.240.sslip.io -p 80
```
