## Basic usage
Start the benchmark tool:
~~~
bash ./benchmark.sh -cf /path/to/config.xml
~~~
The "conf" dir contains predefined configurations files for supported TSDBs, as well as a XSD schema definition.

You should also download the Vertica JDBC dependency from [here](https://www.vertica.com/download/vertica/client-drivers/) and install it locally with Maven.

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