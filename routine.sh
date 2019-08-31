#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CONFIG_PATH="conf/config.properties"
FILENAME="routine"
COUNTER=0

while IFS= read -r line
do
    sh "./benchmark.sh"
done < "$DIR/$FILENAME"




