#!/bin/sh

if [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "" ]; then
  echo "Usage: $(basename "$0") -cf <path to config file>"
  exit 0
else
  if [ "$1" == "-cf" ] && [ "$2" != "" ]; then
    config=$2
    rm -rf lib
    mvn clean package -Dmaven.test.skip=true
    sh bin/startup.sh -cf "${config}"
  fi
fi
