#!/bin/bash

set -x
kubectl delete namespace airflow
./scripts/setup_airflow.sh

# collect data from modified Airflow with Knative
log_dir=./benchmarking_logs_"$(date +%s)"
mkdir -p "$log_dir"

for benchmark in $(cd workflows/knative_yamls && ls | grep benchmark); do
	echo Doing benchmark "$benchmark"
	kn service delete -n airflow --all
	sleep 5
	while [[ $(kubectl -n airflow get pods | grep airflow-worker.*Terminating) ]]; do sleep 1; done
	while [[ ! $(kubectl -n airflow get pods | grep webserver.*1/1.*Running) ]]; do sleep 1; done
	kn service apply -f workflow-gateway/workflow-gateway.yaml -n airflow
	./scripts/deploy_workflow.sh "$benchmark"
	scheduler="$(kubectl -n airflow get pods | grep scheduler | awk '{print $1}')"
	GATEWAY_URL='http://airflow-workflow-gateway.airflow.192.168.1.240.sslip.io'
	for i in {1..5}; do
		echo running iteration "$i"
		curl -u admin:admin -X POST -H 'application/json' --data '{}' "$GATEWAY_URL"/runWorkflow/"$benchmark"
		kubectl -n airflow logs "$scheduler" scheduler | grep TIMING > "$log_dir"/log_timing_"$benchmark"_"$i".txt
		sleep 1
	done
	kubectl -n airflow logs $scheduler scheduler > "$log_dir"/log_scheduler_"$benchmark".txt
	gateway="$(kubectl -n airflow get pods | grep gateway | awk '{print $1}')"
	kubectl -n airflow logs "$gateway" user-container > "$log_dir"/log_gateway_"$benchmark".txt
	sleep 1
done

# collect data for stock Airflow deployment
log_dir=./benchmarking_logs_stock_"$(date +%s)"
mkdir -p "$log_dir"

kubectl delete namespace airflow
./scripts/setup_stock_airflow.sh
kn service apply -f workflow-gateway/workflow-gateway.yaml -n airflow
for benchmark in $(cd workflows/knative_yamls && ls | grep benchmark); do
	echo Doing benchmark "$benchmark"
	sleep 5
	scheduler="$(kubectl -n airflow get pods | grep scheduler | awk '{print $1}')"
	GATEWAY_URL='http://airflow-workflow-gateway.airflow.192.168.1.240.sslip.io'
	for i in {1..5}; do
		echo running iteration "$i"
		curl -u admin:admin -X POST -H 'application/json' --data '{}' "$GATEWAY_URL"/runWorkflow/"$benchmark"
		kubectl -n airflow logs "$scheduler" scheduler | grep TIMING > "$log_dir"/log_timing_"$benchmark"_"$i".txt
		sleep 1
	done
	kubectl -n airflow logs $scheduler scheduler > "$log_dir"/log_scheduler_"$benchmark".txt
	gateway="$(kubectl -n airflow get pods | grep gateway | awk '{print $1}')"
	kubectl -n airflow logs "$gateway" user-container > "$log_dir"/log_gateway_"$benchmark".txt
	sleep 1
done
