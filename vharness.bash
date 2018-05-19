#!/bin/bash

java -XX:+UseSerialGC -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -cp target/benchmarks.jar -Dipc.pub.cpu=1 -Dipc.echo.cpu=2 -Dipc.sub.cpu=3 -Dipc.msgCount=33554432 -Dipc.bufferSize=131072  com.aitusoftware.messaging.ipc.Harness
