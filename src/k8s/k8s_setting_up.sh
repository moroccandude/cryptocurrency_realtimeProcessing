# Copy miniKube on Linux
curl -LO https://github.com/kubernetes/minikube/releases/latest/download/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64

#run cluster with docker driver or virtualMachine
minikube start

# Check the status of the cluster like numbers of nodes
kubectl get node
