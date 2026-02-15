#!/bin/zsh

./target/release/lnc-bench --endpoint 10.0.10.11:1992 --topic-name my-test --duration 15 --connections 16
#./target/release/lnc-chaos --endpoint 10.0.10.11:1992 --statefulset lance --namespace rithmic --topic chaos-test --rate 60000 --warmup-secs 5 --post-roll-secs 8
