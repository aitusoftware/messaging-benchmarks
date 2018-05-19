#!/bin/bash

java -XX:+UseSerialGC -XX:+PrintGCDetails -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -Dipc.pub.delayNs=50000 -cp target/benchmarks.jar -Dipc.pub.cpu=1 -Dipc.echo.cpu=2 -Dipc.sub.cpu=3 -Dipc.msgCount=2097152 -Dipc.bufferSize=131072  com.aitusoftware.messaging.ipc.Harness
