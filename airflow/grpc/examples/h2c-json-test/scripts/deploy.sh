DOCKER_USER='whonamihwp'
DOCKER_NAME='h2c-kn'
YAML_NAME='config.yaml'

echo "Build Docker [NAME: ${DOCKER_USER}/${DOCKER_NAME}]"
docker build -t ${DOCKER_USER}/${DOCKER_NAME} .
docker push ${DOCKER_USER}/${DOCKER_NAME}

kubectl apply -f ${YAML_NAME}

sleep 5s
echo $(kubectl get ksvc -n airflow-grpc airflow-grpc-test-v2 -o jsonpath='{.status.url}')