#!/bin/zsh


NAMESPACE="${LANCE_NAMESPACE:-lance}"
STATEFULSET="${LANCE_STATEFULSET:-lance}"

echo "Waiting for StatefulSet ${STATEFULSET} in namespace ${NAMESPACE} to be ready..."
kubectl rollout status "statefulset/${STATEFULSET}" -n "${NAMESPACE}" --timeout=240s
kubectl wait --for=condition=Ready pod -l app=lance -n "${NAMESPACE}" --timeout=180s

# Allow brief post-ready stabilization for leader election/peer reconnect convergence.
sleep 20

echo "Starting benchmark..."
./target/release/lnc-bench --endpoint 10.0.10.11:1992 --topic-name my-test --duration 15 --connections 16

echo "Starting chaos..."
./target/release/lnc-chaos --endpoint 10.0.10.11:1992 --statefulset lance --namespace lance --topic chaos-test --rate 60000 --warmup-secs 5 --post-roll-secs 8 --duration 15
