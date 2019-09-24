package de.uni_passau.dbts.benchmark.workload.schema;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bike {

  private static final Logger LOGGER = LoggerFactory.getLogger(Bike.class);

  /** Prefix to use for each group's name. */
  private static final String GROUP_NAME_PREFIX = "group_";

  /** Prefix to use for each bike's name. */
  private static final String DEVICE_NAME_PREFIX = "bike_";

  /** Config singleton. */
  private static Config config = ConfigParser.INSTANCE.config();

  /** Each bike belongs to a group. */
  private String group;

  /** Name of the bike. */
  private String name;

  /** List of sensors which the bike is equipped with. */
  private List<Sensor> sensors;

  /** Index of the bike. */
  private int bikeIdx;

  /** Owner's name */
  private String ownerName;

  /**
   * Creates a new bike.
   *
   * @param bikeIdx Index of the new bike.
   * @param ownerName Owner's name.
   */
  public Bike(int bikeIdx, String ownerName) {
    this.bikeIdx = bikeIdx;
    this.name = DEVICE_NAME_PREFIX + bikeIdx;
    this.ownerName = ownerName;
    sensors = new ArrayList<>();
    createEvenlyAllocBikeSchema();
  }

  /**
   * Creates a new bike.
   *
   * @param group Group name the bike belongs to.
   * @param name Name of the bike.
   * @param sensors List of sensors the bike is equipped with.
   * @param ownerName Owner's name.
   */
  public Bike(String group, String name, List<Sensor> sensors, String ownerName) {
    this.group = GROUP_NAME_PREFIX + group;
    this.name = DEVICE_NAME_PREFIX + name;
    this.ownerName = ownerName;
    setSensors(sensors);
  }

  /**
   * Allocates the bike to a group the way that each group preferably contains an even number
   * of bikes.
   */
  private void createEvenlyAllocBikeSchema() {
    int thisDeviceGroupIndex = bikeIdx % config.DEVICE_GROUPS_NUMBER;
    group = GROUP_NAME_PREFIX + thisDeviceGroupIndex;
    setSensors(config.SENSORS);
  }

  /**
   * Returns the name of the bike.
   *
   * @return Name of the bike.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets a new name.
   *
   * @param name New name.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the group which the bike belongs to.
   *
   * @return NName of the group.
   */
  public String getGroup() {
    return group;
  }

  /**
   * Assigns the bike to another group.
   *
   * @param group Name of the new group.
   */
  public void setGroup(String group) {
    this.group = group;
  }

  /**
   * Returns list of sensors which the bike is equipped with.
   *
   * @return List of sensors.
   */
  public List<Sensor> getSensors() {
    return sensors;
  }

  /**
   * Deeply copies the given list of sensors and applies it to the bike.
   *
   * @param sensors List of sensors.
   */
  public void setSensors(List<Sensor> sensors) {
    this.sensors = new ArrayList<>(sensors.size());
    for (Sensor sensor : sensors) {
      if (sensor instanceof GpsSensor) {

        this.sensors.add(new GpsSensor((GpsSensor) sensor, bikeIdx));
      } else {
        this.sensors.add(new BasicSensor((BasicSensor) sensor));
      }
    }
  }

  /**
   * Returns the name of the bike owner.
   *
   * @return Name of the bike owner.
   */
  public String getOwnerName() {
    return ownerName;
  }

  /**
   * Sets a new bike owner.
   *
   * @param ownerName Name of a new bike owner.
   */
  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }
}
