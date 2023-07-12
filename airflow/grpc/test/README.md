## Scripts
```
$ ./scripts/deploy.sh   # Deploy pod on the Knative clusters
$ ./scripts/delete.sh   # Delete pod
```

## Experiments
```
$ echo $(kubectl get ksvc -n h2c-connector h2c-connector-test-v1 -o jsonpath='{.status.url}')
http://h2c-connector-test-v1.h2c-connector.192.168.1.240.sslip.io
$ python h2c_client.py -s http://h2c-connector-test-v1.h2c-connector.192.168.1.240.sslip.io -p 80
```

