package de.uni_passau.dbts.benchmark.workload.schema;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.utils.NameGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The schema of bikes used in the benchmarks.
 */
public class BikesSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(BikesSchema.class);

  /** Config singleton. */
  private static Config config = ConfigParser.INSTANCE.config();

  /** Determines which bikes a worker thread must process. */
  private static final Map<Integer, List<Bike>> CLIENT_BIND_SCHEMA = new HashMap<>();

  /** Random names generator. */
  private NameGenerator nameGenerator = NameGenerator.INSTANCE;

  /**
   * Returns the mapping of worker threads to bikes.
   * @return
   */
  public Map<Integer, List<Bike>> getClientBindSchema() {
    return CLIENT_BIND_SCHEMA;
  }

  /**
   * Creates a new instance.
   */
  private BikesSchema() {
    createClientBindSchema();
  }

  /**
   * Returns a singleton instance.
   *
   * @return Singleton instance.
   */
  public static BikesSchema getInstance() {
    return DataSchemaHolder.INSTANCE;
  }

  /**
   * Stores a reference to the {@link BikesSchema} singleton.
   */
  private static class DataSchemaHolder {

    /** Singleton instance. */
    private static final BikesSchema INSTANCE = new BikesSchema();
  }

  /**
   * Binds bikes to worker threads. Thus, each worker thread has to process a fixed list of
   * bikes.
   */
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
        bikeList.add(new Bike(deviceId++, nameGenerator.getName()));
      }
      if (clientId < mod) {
        bikeList.add(new Bike(deviceId++, nameGenerator.getName()));
      }
      CLIENT_BIND_SCHEMA.put(clientId, bikeList);
    }
  }
}
