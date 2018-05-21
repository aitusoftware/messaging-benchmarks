#!/bin/bash

chmod +x $1

SCRIPT_DIR=$(dirname "$0")

cset proc --exec $USER_CPUSET $1
