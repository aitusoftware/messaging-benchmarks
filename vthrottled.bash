#!/bin/bash

taskset -c $POOL_CPUS java -XX:+UseSerialGC -XX:+PrintGCDetails -XX:+UnlockDiagnosticVMOptions  -Xlog:safepoint:file=/tmp/gc.log -XX:GuaranteedSafepointInterval=300000 -XX:+DebugNonSafepoints -Dipc.pub.delayNs=50000 -cp /home/mark/Code/messaging-benchmarks/target/benchmarks.jar -Dipc.pub.cpu=1 -Dipc.echo.cpu=2 -Dipc.sub.cpu=3 -Dipc.msgCount=200000 -Dipc.bufferSize=131072  com.aitusoftware.messaging.ipc.Harness
