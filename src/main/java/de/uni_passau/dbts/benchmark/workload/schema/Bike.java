package de.uni_passau.dbts.benchmark.workload.schema;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bike {

  private static final Logger LOGGER = LoggerFactory.getLogger(Bike.class);
  private static Config config = ConfigParser.INSTANCE.config();
  public static final String GROUP_NAME_PREFIX = "group_";
  private static final String DEVICE_NAME_PREFIX = "bike_";

  // each device belongs to one group, i.e., database
  private String group;

  // bikeId
  private String device;

  // sensorIds
  private List<Sensor> sensors;

  // only for synthetic data set
  private int bikeId;

  private String ownerName;

  public Bike(int bikeId, String ownerName) {
    this.bikeId = bikeId;
    this.device = DEVICE_NAME_PREFIX + bikeId;
    this.ownerName = ownerName;
    sensors = new ArrayList<>();
    createEvenlyAllocBikeSchema();
  }

  public Bike(String group, String device, List<Sensor> sensors, String ownerName) {
    this.group = GROUP_NAME_PREFIX + group;
    this.device = DEVICE_NAME_PREFIX + device;
    this.ownerName = ownerName;
    setSensors(sensors);
  }

  private void createEvenlyAllocBikeSchema() {
    int thisDeviceGroupIndex =
        calGroupId(bikeId, config.DEVICES_NUMBER, config.DEVICE_GROUPS_NUMBER);
    group = GROUP_NAME_PREFIX + thisDeviceGroupIndex;
    setSensors(config.SENSORS);
  }

  static int calGroupId(int bikeId, int deviceNum, int groupNum) {
    return bikeId % groupNum;
  }

  public String getName() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public List<Sensor> getSensors() {
    return sensors;
  }

  public void setSensors(List<Sensor> sensors) {
    this.sensors = new ArrayList<>(sensors.size());
    for (Sensor sensor : sensors) {
      if (sensor instanceof GpsSensor) {

        this.sensors.add(new GpsSensor((GpsSensor) sensor, bikeId));
      } else {
        this.sensors.add(new BasicSensor((BasicSensor) sensor));
      }
    }
  }

  public String getOwnerName() {
    return ownerName;
  }

  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }
}
