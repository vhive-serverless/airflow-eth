kn service delete --all -n airflow
kubectl delete namespace airflow
kubectl delete -f configs/volumes.yaml
sudo rm -rf /mnt/data*/*

./scripts/build_knative_yamls.sh
./scripts/update_images.sh
# ./scripts/build_profiling_image.sh
docker tag airflow-worker:latest nehalem90/airflow-worker:latest
docker push nehalem90/airflow-worker:latest
docker tag airflow:latest nehalem90/airflow:latest
docker push nehalem90/airflow:latest
./scripts/setup_airflow.sh

# Get Logs
log_dir=./benchmark/"$(date +%s)"
mkdir -p "$log_dir"
GATEWAY_URL="$(kn service list -o json -n airflow | jq -r '.items[] | select(.metadata.name=="airflow-workflow-gateway").status.url')"
curl -u admin:admin -X POST -H 'application/json' --data '{"input": [1,2,3,4]}' "$GATEWAY_URL"/runWorkflow/benchmark_w1_d2 &
(sleep 15
scheduler="$(kubectl -n airflow get pods | grep scheduler | awk '{print $1}')"
kubectl -n airflow logs "$scheduler" scheduler | grep TIMING > "$log_dir"/log_timing.log
kubectl -n airflow logs "$scheduler" scheduler > "$log_dir"/log_scheduler.log
gateway="$(kubectl -n airflow get pods | grep gateway | awk '{print $1}')"
kubectl -n airflow logs "$gateway" user-container > "$log_dir"/log_gateway.log
producer="$(kubectl -n airflow get pods | grep extract | awk '{print $1}')"
kubectl -n airflow logs "$producer" user-container > "$log_dir"/log_producer.log
consumer="$(kubectl -n airflow get pods | grep do-sum | awk '{print $1}')"
kubectl -n airflow logs "$consumer" user-container > "$log_dir"/log_consumer.log)