#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
FILENAME="routine"


while IFS= read -r line
do
    export IFS=" "
    for setting in $line; do
        if [ "$setting" == "TEST" ]; then
            sh "cli-benchmark.sh"
        else
            IFS="="
            read -ra parts <<< "$setting"

            #printf "${parts[0]}\n"
            sed -i '' "s/${parts[0]}=.*/${setting}/" ${DIR}'/conf/config.properties'
        fi
    done
done < "$DIR/$FILENAME"




