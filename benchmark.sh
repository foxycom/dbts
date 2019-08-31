#!/bin/sh

#git pull

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rm -rf lib
mvn clean package -Dmaven.test.skip=true
cd bin
sh startup.sh -cf ../conf/config.xml
