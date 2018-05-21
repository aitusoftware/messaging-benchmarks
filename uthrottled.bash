#!/bin/bash

taskset -c $POOL_CPUS java -XX:+UseSerialGC -XX:+UnlockDiagnosticVMOptions -XX:GuaranteedSafepointInterval=300000 -XX:+PrintGCDetails -XX:+DebugNonSafepoints -Dipc.pub.delayNs=50000 -cp /home/mark/Code/messaging-benchmarks/target/benchmarks.jar -Dipc.pub.cpu=1 -Dipc.echo.cpu=2 -Dipc.sub.cpu=3 -Dipc.msgCount=400000 -Dipc.bufferSize=131072 -Dagrona.disable.bounds.checks=true com.aitusoftware.messaging.ipc.UnsafeHarness
