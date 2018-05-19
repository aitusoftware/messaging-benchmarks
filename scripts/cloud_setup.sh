#!/bin/bash
set -euxo pipefail
sudo apt update
sudo apt install -y cpuset openjdk-10-jdk-headless git hwloc unzip

cd

lstopo-no-graphics --of xml -v topo.xml

git clone https://github.com/aitusoftware/messaging-benchmarks.git

export OVERRIDE_IRQ_CPUS=3,4,5,21,22,23
export OVERRIDE_RESTRICTED_CPUS=11,12,13,14,15,16,17,29,30,31,32,33,34,35
export OVERRIDE_SYSTEM_CPUS=0,1,2,18,19,20
export OVERRIDE_CLIENT_IN_CPU=14
export OVERRIDE_CLIENT_OUT_CPU=15
export OVERRIDE_SERVER_IN_CPU=16
export OVERRIDE_SERVER_OUT_CPU=17
export OVERRIDE_POOL_CPUS=11,12,13,29,30,31
export POOL_CPUS=${OVERRIDE_POOL_CPUS:-"1,2,3"}
export USER_CPUSET=${OVERRIDE_USER_CPUSET:-"client_set"}

bash ./unmask_cpus.sh
bash ./mask_cpus.sh

bash ./cpuset_wrapper.sh ../uthrottled.bash
#bash ./cpuset_wrapper.sh ../vthrottled.bash
