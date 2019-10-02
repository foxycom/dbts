## Basic usage
Start the benchmark tool:
~~~
benchmark.sh -cf /path/to/config.xml
~~~
The "conf" dir contains predefined configurations files for supported TSDBs, as well as a XSD schema definition.

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