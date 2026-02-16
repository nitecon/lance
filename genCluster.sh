#!/bin/zsh

kubectl delete -f k8s/lance-cluster.yaml && kubectl delete pvc data-lance-0 data-lance-1 data-lance-2 && sleep 10 && kubectl apply -f k8s/lance-cluster.yaml