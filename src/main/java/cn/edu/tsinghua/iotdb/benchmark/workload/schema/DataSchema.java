package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSchema.class);
    private static Config config = ConfigParser.INSTANCE.config();
  private static final Map<Integer, List<Bike>> CLIENT_BIND_SCHEMA = new HashMap<>();

  public Map<Integer, List<Bike>> getClientBindSchema() {
    return CLIENT_BIND_SCHEMA;
  }

  private DataSchema(){
    createClientBindSchema();
  }

  public static DataSchema getInstance() {
    return DataSchemaHolder.INSTANCE;
  }

  private static class DataSchemaHolder {
    private static final DataSchema INSTANCE = new DataSchema();
  }

  private void createClientBindSchema() {
    int eachClientDeviceNum = 0;
    if (config.CLIENTS_NUMBER != 0) {
      eachClientDeviceNum = config.DEVICES_NUMBER / config.CLIENTS_NUMBER;
    } else {
      LOGGER.error("CLIENT_NUMBER can not be zero.");
      return;
    }

    int deviceId = 0;
    int mod = config.DEVICES_NUMBER % config.CLIENTS_NUMBER;
    for (int clientId = 0; clientId < config.CLIENTS_NUMBER; clientId++) {
      List<Bike> bikeList = new ArrayList<>();
      for (int j = 0; j < eachClientDeviceNum; j++) {
        bikeList.add(new Bike(deviceId++));
      }
      if (clientId < mod) {
        bikeList.add(new Bike(deviceId++));
      }
      CLIENT_BIND_SCHEMA.put(clientId, bikeList);
    }
  }
}
