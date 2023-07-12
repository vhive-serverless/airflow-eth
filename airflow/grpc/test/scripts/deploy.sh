NAMESPACE='remote-xcomm'
DOCKER_USER='whonamihwp'
DOCKER_NAME="${NAMESPACE}-test"
YAML_NAME='config.yaml'

LIB="../remote_xcomm"
cp -r ${LIB} ./

echo "Build Docker [NAME: ${DOCKER_USER}/${DOCKER_NAME}]"
docker build -t ${DOCKER_USER}/${DOCKER_NAME} .
docker push ${DOCKER_USER}/${DOCKER_NAME}

kubectl apply -f ${YAML_NAME}

sleep 5s
echo $(kubectl get ksvc -n ${NAMESPACE} ${NAMESPACE}-test-v1 -o jsonpath='{.status.url}')