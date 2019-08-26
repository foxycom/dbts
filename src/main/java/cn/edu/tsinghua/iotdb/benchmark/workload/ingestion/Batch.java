package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;

import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Batch {

  private DeviceSchema deviceSchema;
  private List<Record> records;
  private long timeRange;
  private Map<Sensor, List<Point>> entries = new HashMap<>();

  public Batch() {
    records = new ArrayList<>();
  }

  public Batch(DeviceSchema deviceSchema, List<Record> records) {
    this.deviceSchema = deviceSchema;
    this.records = records;
  }

  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }

  public void setDeviceSchema(DeviceSchema deviceSchema) {
    this.deviceSchema = deviceSchema;
  }

  public List<Record> getRecords() {
    return records;
  }

  public void add(long timestamp, List<String> values) {
    records.add(new Record(timestamp, values));
  }

  public void add(Sensor sensor, List<Point> values) {
    if (!entries.containsKey(sensor)) {
      entries.put(sensor, values);
    } else {
      System.err.println("BATCH ALREADY CONTAINS SENSOR DATA");
      System.exit(2);
    }
  }

  /**
   * use the row protocol which means data are organized in List[timestamp, List[value]]
   *
   * @return data point number in this batch
   */
  public int pointNum() {
    int pointNum = 0;
    for (Sensor sensor : entries.keySet()) {
      pointNum += entries.get(sensor).size();
    }
    return pointNum;
  }

  public void setTimeRange(long timeRange) {
    this.timeRange = timeRange;
  }

  public long getTimeRange() {
    return this.timeRange;
  }

  public Map<Sensor, List<Point>> getEntries() {
    return entries;
  }
}
