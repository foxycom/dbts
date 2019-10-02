package de.uni_passau.dbts.benchmark.conf;

import de.uni_passau.dbts.benchmark.enums.Aggregation;
import de.uni_passau.dbts.benchmark.enums.SensorType;
import de.uni_passau.dbts.benchmark.function.Function;
import de.uni_passau.dbts.benchmark.function.GeoFunction;
import de.uni_passau.dbts.benchmark.tsdb.DB;
import de.uni_passau.dbts.benchmark.workload.schema.BasicSensor;
import de.uni_passau.dbts.benchmark.workload.schema.GpsSensor;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import de.uni_passau.dbts.benchmark.workload.schema.SensorGroup;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * Parser of XML configuration files.
 */
public enum ConfigParser {
  INSTANCE;

  private Config config;

  ConfigParser() {
    config = new Config();
    load();
  }

  public Config config() {
    return config;
  }

  /**
   * Loads config parameters from an XML file into the Config singleton. The configuration must
   * comply to the config.xsd schema.
   */
  private void load() {
    String xmlPath = System.getProperty(Constants.BENCHMARK_CONF);
    File configFile = new File(xmlPath);
    Configurations configurations = new Configurations();
    XMLConfiguration xml = null;
    try {
      xml = configurations.xml(configFile);
    } catch (ConfigurationException e) {
      System.err.println(e.getMessage());
      System.err.println("Could not parse config.");
      e.printStackTrace();
      System.exit(1);
    }

    config.DB_SWITCH = DB.valueOf(xml.getString("db.type", config.DB_SWITCH.name()).toUpperCase());
    config.DB_NAME = xml.getString("db.name", config.DB_NAME);
    config.HOST = xml.getString("db.host", config.HOST);
    config.PORT = xml.getString("db.port", config.PORT);
    config.ERASE_DATA = xml.getBoolean("db.erase", config.ERASE_DATA);
    config.ERASE_WAIT_TIME = xml.getLong("db.erase[@wait]", config.ERASE_WAIT_TIME);
    config.CREATE_SCHEMA = xml.getBoolean("db.createSchema", config.CREATE_SCHEMA);

    config.READ_TOKEN = xml.getString("warp10.readToken", config.READ_TOKEN);
    config.WRITE_TOKEN = xml.getString("warp10.writeToken", config.WRITE_TOKEN);

    config.WORK_MODE = Mode.valueOf(xml.getString("mode", config.WORK_MODE.name()).toUpperCase());
    config.MYSQL_URL = xml.getString("mysqlLog", config.MYSQL_URL);
    config.USE_MYSQL = xml.getBoolean("mysqlLog[@active]", config.USE_MYSQL);
    config.REMARK = xml.getString("mysqlLog[@remark]");
    config.CLIENTS_NUMBER = xml.getInt("clients", config.CLIENTS_NUMBER);
    config.BIND_CLIENTS_TO_DEVICES =
        xml.getBoolean("clients[@bindToDevices]", config.BIND_CLIENTS_TO_DEVICES);
    config.DEVICE_GROUPS_NUMBER = xml.getInt("deviceGroups", config.DEVICE_GROUPS_NUMBER);
    config.DEVICES_NUMBER = xml.getInt("devices", config.DEVICES_NUMBER);

    config.OPERATION_PROPORTION =
        xml.getString("operationsProportion", config.OPERATION_PROPORTION);

    config.BATCH_SIZE = xml.getInt("batch[@size]", config.BATCH_SIZE);
    config.LOOP = xml.getLong("loops", config.LOOP);
    config.MONITOR_SERVER = xml.getBoolean("serverMonitor[@active]", config.MONITOR_SERVER);
    config.DRIVE_NAME = xml.getString("serverMonitor.drive");
    config.IFACE_NAME = xml.getString("serverMonitor.iface");
    config.SERVER_MONITOR_PORT = xml.getInt("serverMonitor.port");

    config.USE_OVERFLOW = xml.getBoolean("overflow[@active]", config.USE_OVERFLOW);
    config.OVERFLOW_RATIO = xml.getDouble("overflow.ratio", config.OVERFLOW_RATIO);
    config.LAMBDA = xml.getDouble("overflow.lambda", config.LAMBDA);
    config.MAX_K = xml.getInt("overflow.maxK", config.MAX_K);

    config.START_TIMESTAMP_INDEX = xml.getInt("timestamp.startIndex", config.START_TIMESTAMP_INDEX);
    config.DATA_SEED = xml.getLong("timestamp.seed", config.DATA_SEED);

    // TODO reimplement.
    config.QUERY_CHOICE = xml.getInt("query.type", config.QUERY_CHOICE);

    config.QUERY_SENSOR_NUM = xml.getInt("query.sensorNum", config.QUERY_SENSOR_NUM);
    config.QUERY_SENSOR_GROUP = xml.getString("query.sensorGroup");
    config.QUERY_DEVICE_NUM = xml.getInt("query.deviceNum", config.QUERY_DEVICE_NUM);
    config.QUERY_SEED = xml.getLong("query.seed", config.QUERY_SEED);
    config.STEP_SIZE = xml.getInt("query.step", config.STEP_SIZE);
    config.RADIUS = xml.getInt("query.radius", config.RADIUS);
    config.QUERY_AGGREGATE_FUN =
        Aggregation.valueOf(xml.getString("query.aggregateFunction").toUpperCase());
    config.QUERY_INTERVAL = xml.getLong("query.interval", config.INTERVAL);
    config.QUERY_LOWER_LIMIT = xml.getDouble("query.lowerLimit", config.QUERY_LOWER_LIMIT);
    config.TIME_BUCKET = xml.getLong("query.timeBucket", config.TIME_BUCKET);

    initSensors(xml);
  }

  /**
   * Creates sensor groups and corresponding objects based on the configuration file. Each sensor
   * must belong to a group, have a frequency, and have a type. A sensor group must specify a data
   * type, which is applied to all sensors in the group. Additionally, a sensor group might specify
   * custom fields names, so each sensor yields several values.
   *
   * @param xml The XML config instance.
   */
  private void initSensors(XMLConfiguration xml) {
    String sensorNamePrefix = xml.getString("sensorGroups[@prefix]");
    List<HierarchicalConfiguration<ImmutableNode>> sensorGroups =
        xml.configurationsAt("sensorGroups.sensorGroup");

    for (int i = 0; i < sensorGroups.size(); i++) {
      String sensorGroupName = sensorGroups.get(i).getString("[@name]");
      String sensorGroupDataType = sensorGroups.get(i).getString("[@dataType]");
      String sensorGroupFields = sensorGroups.get(i).getString("[@fields]", "");
      SensorGroup sensorGroup = new SensorGroup(sensorGroupName);
      config.SENSOR_GROUPS.add(sensorGroup);
      List<HierarchicalConfiguration<ImmutableNode>> sensors =
          sensorGroups.get(i).configurationsAt("sensor");
      for (int j = 0; j < sensors.size(); j++) {

        HierarchicalConfiguration<ImmutableNode> sensorConfig = sensors.get(j);
        int sensorIndex = config.SENSORS.size();
        String name = sensorNamePrefix + sensorIndex;
        int freq = sensorConfig.getInt("[@frequency]");
        SensorType type = SensorType.valueOf(sensorConfig.getString("[@type]").toUpperCase());

        Sensor sensor;
        switch (type) {
          case BASIC:
            if (sensorGroupFields.equals("")) {
              sensor =
                  new BasicSensor(name, sensorGroup, new Function(j), freq, sensorGroupDataType);
            } else {
              List<String> fields = Arrays.asList(sensorGroupFields.split(", "));
              sensor =
                  new BasicSensor(
                      name, sensorGroup, new Function(j), freq, sensorGroupDataType, fields);
            }
            config.SENSORS.add(sensor);
            break;
          case GPS:
            if (sensorGroupFields.equals("")) {
              sensor =
                  new GpsSensor(name, sensorGroup, new GeoFunction(j), freq, sensorGroupDataType);
            } else {
              List<String> fields = Arrays.asList(sensorGroupFields.split(", "));
              sensor =
                  new GpsSensor(
                      name, sensorGroup, new GeoFunction(j), freq, sensorGroupDataType, fields);
            }
            config.SENSORS.add(sensor);
            break;
          default:
            throw new IllegalStateException("Inappropriate sensor type.");
        }

        sensorGroup.addSensor(sensor);
      }
    }
  }
}
