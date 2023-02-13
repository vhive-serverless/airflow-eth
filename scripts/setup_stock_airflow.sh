#!/bin/bash

# increase max open files
ulimit -n 1000000

# install helm
curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
sudo apt-get install apt-transport-https --yes
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
sudo apt-get update
sudo apt-get install helm

# setup volumes
kubectl create namespace airflow
sudo mkdir -p /mnt/data{0..19}
sudo chmod 777 /mnt/data*
kubectl -n airflow apply -f configs/volumes.yaml

# create resource files from airflow helm chart
helm repo add apache-airflow https://airflow.apache.org
helm template airflow apache-airflow/airflow --version 1.7.0 --namespace airflow -f configs/values-stock.yaml --debug > configs/airflow-stock.yaml

# deploy airflow
kubectl -n airflow apply -f configs/airflow-stock.yaml
while [[ ! $(kubectl -n airflow get pods | grep scheduler.*Running) ]]; do sleep 1; done

# deploy workflow gateway
# this is the service that lets users run workflows and returns the results
kn service apply -f workflow-gateway/workflow-gateway.yaml -n airflow

# wait for webserver
while [[ ! $(kubectl -n airflow get pods | grep webserver.*1/1.*Running) ]]; do sleep 1; done
