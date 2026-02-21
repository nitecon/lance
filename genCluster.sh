#!/bin/zsh

set -euo pipefail

if kubectl get -f k8s/lance-cluster.yaml >/dev/null 2>&1; then
  echo "Existing example cluster found; deleting before recreate..."
  kubectl delete -f k8s/lance-cluster.yaml
  kubectl delete pvc data-lance-0 data-lance-1 data-lance-2 --ignore-not-found=true
  sleep 10
else
  echo "No existing example cluster found; creating fresh cluster..."
fi

kubectl apply -f k8s/lance-cluster.yaml