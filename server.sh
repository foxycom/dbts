#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CONFIG_PATH="conf/config.properties"

sed -i '' "s/BENCHMARK_WORK_MODE=.*/BENCHMARK_WORK_MODE=server_mode/" "$DIR/$CONFIG_PATH"
sh "cli-benchmark.sh"