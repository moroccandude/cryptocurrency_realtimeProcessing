#Create the namespace:
bash kubectl apply -f namespace.yaml

#Deploy your services in order:
# Start data stores first
kubectl apply -f zookeeper-deployment.yaml
kubectl apply -f zookeeper-service.yaml
kubectl apply -f cassandra-statefulset.yaml
kubectl apply -f cassandra-service.yaml

# Then messaging infrastructure
kubectl apply -f kafka-statefulset.yaml
kubectl apply -f kafka-service.yaml

# Then processing infrastructure
kubectl apply -f spark-master-deployment.yaml
kubectl apply -f spark-master-service.yaml
kubectl apply -f spark-worker-1-deployment.yaml
kubectl apply -f spark-worker-2-deployment.yaml

# Finally, your application
kubectl apply -f fetcher-configmap.yaml
kubectl apply -f fetcher-deployment.yaml
kubectl apply -f fetcher-service.yaml