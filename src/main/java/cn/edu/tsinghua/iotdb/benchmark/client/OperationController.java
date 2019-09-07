package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationController {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationController.class);
    private static Config config = ConfigParser.INSTANCE.config();
  private Random random;

  OperationController(int seed) {
    random = new Random(seed);
  }

  /**
   * @return Operation the next operation for client to execute
   */
  Operation getNextOperationType() {
    List<Double> proportion = resolveOperationProportion();
    double[] p = new double[Operation.values().length + 1];

    // split [0,1] to n regions, each region corresponds to a operation type whose probability
    // is the region range size.
    for (int i = 1; i <= Operation.values().length; i++) {
      p[i] = p[i - 1] + proportion.get(i - 1);
    }
    double rand = random.nextDouble();
    int i;
    for (i = 1; i <= Operation.values().length; i++) {
      if (rand >= p[i - 1] && rand <= p[i]) {
        break;
      }
    }
    switch (i) {
      case 1:
        return Operation.INGESTION;
      case 2:
        return Operation.PRECISE_POINT;
      case 3:
        return Operation.ACTIVE_BIKES;
      case 4:
        return Operation.AIRQUALITY_HEATMAP;
      case 5:
        return Operation.DISTANCE_DRIVEN;
      case 6:
        return Operation.LAST_KNOWN_POSITION;
      case 7:
        return Operation.IDENTIFY_TRIPS;
      case 8:
        return Operation.BIKES_IN_LOCATION;
      case 9:
        return Operation.GPS_PATH_SCAN;
      case 10:
        return Operation.DOWNSAMPLE;
      case 11:
        return Operation.TRAFFIC_JAMS;
      default:
        LOGGER.error("Unsupported operation {}, use default operation: INGESTION.", i);
        return Operation.INGESTION;
    }
  }

  List<Double> resolveOperationProportion() {
    List<Double> proportion = new ArrayList<>();
    String[] split = config.OPERATION_PROPORTION.split(":");
    if (split.length != Operation.values().length) {
      LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
    }
    double[] proportions = new double[Operation.values().length];
    double sum = 0;
    for (int i = 0; i < split.length; i++) {
      proportions[i] = Double.parseDouble(split[i]);
      sum += proportions[i];
    }
    for (int i = 0; i < split.length; i++) {
      if (sum != 0) {
        proportion.add(proportions[i] / sum);
      } else {
        proportion.add(0.0);
        LOGGER.error("The sum of operation proportions is zero!");
      }
    }
    return proportion;
  }

  public enum Operation {
    INGESTION("INGESTION"),
    PRECISE_POINT("PRECISE_POINT"),
    ACTIVE_BIKES("ACTIVE_BIKES"),
    AIRQUALITY_HEATMAP("AIRQUALITY_HEATMAP"),
    DISTANCE_DRIVEN("DISTANCE_DRIVEN"),
    LAST_KNOWN_POSITION("LAST_KNOWN_POSITION"),
    IDENTIFY_TRIPS("IDENTIFY_TRIPS"),
    BIKES_IN_LOCATION("BIKES_IN_LOCATION"),
    GPS_PATH_SCAN("GPS_PATH_SCAN"),
    DOWNSAMPLE("DOWNSAMPLE"),
    TRAFFIC_JAMS("TRAFFIC_JAMS");

    public String getName() {
      return name;
    }

    String name;

    Operation(String name) {
      this.name = name;
    }
  }
}
