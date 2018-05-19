#!/bin/bash

java -jar target/benchmarks.jar -jvmArgsPrepend "-Dbench.affinity=1 -Dagrona.disable.bounds.checks=true -Dipc.disable.subscriberGate=true" "$@"

