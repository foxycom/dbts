package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.tsdb.DB;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.BasicSensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.GpsSensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorType;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Set;

import static cn.edu.tsinghua.iotdb.benchmark.conf.Constants.GEO_DATA_TYPE;

public enum ConfigParser {
    INSTANCE;

    private Config config;

    ConfigParser() {
        config = new Config();
        load();
    }

    private void load() {
        String xmlPath = "/home/tim/iotdb-benchmark/conf/config.xml";
        File configFile = new File(xmlPath);
        Configurations configurations = new Configurations();
        XMLConfiguration xml = null;
        try {
             xml = configurations.xml(configFile);
        } catch (ConfigurationException e) {
            System.err.println("Could not parse config.");
            System.exit(1);
        }

        config.DB_SWITCH = DB.valueOf(xml.getString("db.type", config.DB_SWITCH.name()).toUpperCase());
        config.DB_NAME = xml.getString("db.name", config.DB_NAME);
        config.HOST = xml.getString("db.host", config.HOST);
        config.PORT = xml.getString("db.port", config.PORT);
        config.ERASE_DATA = xml.getBoolean("db.erase", config.ERASE_DATA);
        config.ERASE_WAIT_TIME = xml.getLong("db.erase[@wait]", config.ERASE_WAIT_TIME);
        config.CREATE_SCHEMA = xml.getBoolean("db.createSchema", config.CREATE_SCHEMA);

        config.WORK_MODE = Mode.valueOf(xml.getString("mode", config.WORK_MODE.name()).toUpperCase());
        config.MYSQL_URL = xml.getString("mysqlLog", config.MYSQL_URL);
        config.USE_MYSQL = xml.getBoolean("mysqlLog[@active]", config.USE_MYSQL);
        config.CLIENTS_NUMBER = xml.getInt("clients", config.CLIENTS_NUMBER);
        config.BIND_CLIENTS_TO_DEVICES = xml.getBoolean("clients[@bindToDevices]", config.BIND_CLIENTS_TO_DEVICES);
        config.DEVICE_GROUPS_NUMBER = xml.getInt("deviceGroups", config.DEVICE_GROUPS_NUMBER);
        config.DEVICES_NUMBER = xml.getInt("devices", config.DEVICES_NUMBER);

        config.BATCH_SIZE = xml.getInt("batch[@size]", config.BATCH_SIZE);
        config.LOOP = xml.getLong("loops", config.LOOP);
        config.POINT_STEP = xml.getLong("pointStep", config.POINT_STEP);
        config.ENCODING = xml.getString("encoding", config.ENCODING).toUpperCase();
        config.MONITOR_SERVER = xml.getBoolean("serverMonitor[@active]", config.MONITOR_SERVER);
        config.DRIVE_NAME = xml.getString("serverMonitor.drive");
        config.IFACE_NAME = xml.getString("serverMonitor.iface");
        config.SERVER_MONITOR_PORT = xml.getInt("serverMonitor.port");

        config.MUL_DEV_BATCH = xml.getBoolean("mulDevBatch", config.MUL_DEV_BATCH);
        config.USE_OVERFLOW = xml.getBoolean("overflow[@active]", config.USE_OVERFLOW);
        config.OVERFLOW_RATIO = xml.getDouble("overflow.ratio", config.OVERFLOW_RATIO);
        config.LAMBDA = xml.getDouble("overflow.lambda", config.LAMBDA);
        config.MAX_K = xml.getInt("overflow.maxK", config.MAX_K);

        config.START_TIMESTAMP_INDEX = xml.getInt("timestamp.startIndex", config.START_TIMESTAMP_INDEX);
        config.DATA_SEED = xml.getLong("timestamp.seed", config.DATA_SEED);

        config.LINE_RATIO = xml.getDouble("functions.line", config.LINE_RATIO);
        config.SIN_RATIO = xml.getDouble("functions.sin", config.SIN_RATIO);
        config.SQUARE_RATIO = xml.getDouble("functions.square", config.SQUARE_RATIO);
        config.RANDOM_RATIO = xml.getDouble("functions.random", config.RANDOM_RATIO);
        config.CONSTANT_RATIO = xml.getDouble("functions.constant", config.CONSTANT_RATIO);

        // TODO reimplement
        config.QUERY_CHOICE = xml.getInt("query.type", config.QUERY_CHOICE);
        config.QUERY_SENSOR_NUM = xml.getInt("query.sensorNum", config.QUERY_SENSOR_NUM);
        config.QUERY_DEVICE_NUM = xml.getInt("query.deviceNum", config.QUERY_DEVICE_NUM);
        config.QUERY_SEED = xml.getLong("query.seed", config.QUERY_SEED);
        config.STEP_SIZE = xml.getInt("query.step", config.STEP_SIZE);
        config.QUERY_AGGREGATE_FUN = xml.getString("query.aggregateFunction", config.QUERY_AGGREGATE_FUN);
        config.QUERY_INTERVAL = xml.getLong("query.interval", config.INTERVAL);
        config.QUERY_LOWER_LIMIT = xml.getDouble("query.lowerLimit", config.QUERY_LOWER_LIMIT);
        config.IS_EMPTY_PRECISE_POINT_QUERY = xml.getBoolean("query.emptyPrecisePointQuery",
                config.IS_EMPTY_PRECISE_POINT_QUERY);
        // TODO change name
        config.TIME_UNIT = xml.getLong("query.groupBy", config.TIME_UNIT);
        config.LIMIT_CLAUSE_MODE = xml.getInt("query.limitClause", config.LIMIT_CLAUSE_MODE);
        config.QUERY_LIMIT_N = xml.getInt("query.limitN", config.QUERY_LIMIT_N);
        config.QUERY_LIMIT_OFFSET = xml.getInt("query.limitOffset", config.QUERY_LIMIT_OFFSET);
        config.QUERY_SLIMIT_N = xml.getInt("query.sLimitN", config.QUERY_SLIMIT_N);
        config.QUERY_SLIMIT_OFFSET = xml.getInt("query.sLimitOffset", config.QUERY_SLIMIT_OFFSET);

        initSensorGroups(xml);
        initSensors(xml);
    }

    private void initSensors(XMLConfiguration xml) {
        List<HierarchicalConfiguration<ImmutableNode>> sensorsConfigurations
                = xml.configurationsAt("sensors.sensor");
        String prefix = xml.getString("sensors[@prefix]");
        config.SENSORS_NUMBER = sensorsConfigurations.size();
        for (int i = 0; i < sensorsConfigurations.size(); i++) {
            HierarchicalConfiguration<ImmutableNode> configuration = sensorsConfigurations.get(i);
            String name = prefix + i;
            int freq = configuration.getInt("[@frequency]");
            String dataType = configuration.getString("[@dataType]");
            SensorType type = SensorType.valueOf(configuration.getString("[@type]").toUpperCase());
            String sensorGroup = configuration.getString("[@sensorGroup]");

            switch (type) {
                case BASIC:
                    config.SENSORS.add(new BasicSensor(name, sensorGroup, null, freq, dataType));
                    break;
                case GPS:
                    config.SENSORS.add(new GpsSensor(name, sensorGroup, null, freq, GEO_DATA_TYPE));
                    break;
                default:
                    System.err.println("Inapropriate sensor type.");
            }
        }
    }

    private void initSensorGroups(XMLConfiguration xml) {
        List<HierarchicalConfiguration<ImmutableNode>> sensorGroupsConfigurations
                = xml.configurationsAt("sensorGroups.sensorGroup");
        for (HierarchicalConfiguration<ImmutableNode> configuration : sensorGroupsConfigurations) {
            config.SENSOR_GROUPS.add(configuration.getString(""));
        }
    }

    public static void main(String[] args) {
        ConfigParser parser = ConfigParser.INSTANCE;
    }
}
