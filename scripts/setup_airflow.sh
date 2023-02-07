#!/bin/bash

echo Please paste the credentials that are necessary to pull images.
echo This script will create a Kubernetes Secret named "'regcred'" in the
echo "'airflow'" namespace with your credentials.
echo
echo Username:
read CRED_USER
echo Email:
read CRED_EMAIL
echo Access token:
read CRED_TOKEN

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
helm template airflow apache-airflow/airflow --version 1.7.0 --namespace airflow -f configs/values.yaml --debug > configs/airflow.yaml

# create pull secret
kubectl create secret -n airflow docker-registry regcred --docker-server=https://ghcr.io --docker-username="$CRED_USER" --docker-password="$CRED_TOKEN" --docker-email="$CRED_EMAIL"
# patch resources with reference to pull secret
python3 ./scripts/patch_values.py ./configs/airflow.yaml

# deploy airflow
kubectl -n airflow apply -f configs/airflow.yaml
while [[ ! $(kubectl -n airflow get pods | grep scheduler.*Running) ]]; do sleep 1; done

# provide scheduler access to kubernetes admin interface, this is required to discover knative services
scheduler="$(kubectl -n airflow get pods | grep scheduler | awk '{print $1}')"
kubectl -n airflow exec $scheduler -- mkdir /home/airflow/.kube
kubectl -n airflow cp ~/.kube/config "$scheduler":/home/airflow/.kube/config

# deploy the example workflows
for yaml_file in workflows/avg_distributed/*.yaml; do
	kn service apply -f "$yaml_file" -n airflow
done

# deploy workflow gateway
# this is the service that lets users run workflows and returns the results
kn service apply -f workflow-gateway/workflow-gateway.yaml -n airflow

