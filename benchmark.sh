#!/bin/sh

#git pull

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CONFIG_PATH="conf/config.properties"

sed -i "s/BENCHMARK_WORK_MODE=.*/BENCHMARK_WORK_MODE=test_with_default_path/" "$DIR/$CONFIG_PATH"

rm -rf lib
mvn clean package -Dmaven.test.skip=true
cd bin
sh startup.sh -cf ../conf/monitor.xml
