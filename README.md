## Basic usage
Start the benchmark tool:
~~~
benchmark.sh -cf /path/to/config.xml
~~~
The "conf" dir contains predefined configurations files for supported TSDBs, as well as a XSD schema definition.

You should also install the CrateDB JDBC dependency from [here](https://bintray.com/crate/crate/crate-jdbc).

## Supported DBs
Currently, dbts supports the following databases:
* MemSQL
* InfluxDB
* ClickHouse
* Vertica
* CrateDB
* Citus
* TimescaleDB
* Warp 10