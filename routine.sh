#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CONFIG_PATH="conf/config.properties"
FILENAME="routine"
COUNTER=0

sed -i "s/BENCHMARK_WORK_MODE=.*/BENCHMARK_WORK_MODE=test_with_default_path/" "$DIR/$CONFIG_PATH"

while IFS= read -r line
do
    export IFS=" "
    for setting in $line; do
        if [ "$setting" == "TEST" ]; then
            printf "####################### TEST NUMBER %d #######################\n" $COUNTER
            COUNTER=$((COUNTER+1))
            sh "./cli-benchmark.sh"
        else
            IFS="="
            read -ra parts <<< "$setting"

            #printf "${parts[0]}\n"
            sed -i "s/${parts[0]}=.*/${setting}/" ${DIR}'/conf/config.properties'
        fi
    done
done < "$DIR/$FILENAME"




