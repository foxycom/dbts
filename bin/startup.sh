#!/bin/sh

if [ -z "${BENCHMARK_HOME}" ]; then
  export BENCHMARK_HOME="$(cd "$(dirname "$0")"/.. || exit; pwd)"
fi

echo "$BENCHMARK_HOME"

MAIN_CLASS=de.uni_passau.dbts.benchmark.App

CLASSPATH=""
for f in ${BENCHMARK_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

exec "$JAVA" -Xms3G -Duser.timezone=GMT+2 -Dlogback.configurationFile="${BENCHMARK_HOME}"/conf/logback.xml  -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

exit $?