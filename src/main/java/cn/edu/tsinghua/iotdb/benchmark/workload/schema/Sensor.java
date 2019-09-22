package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DB;

import java.util.List;

public interface Sensor {
  String getName();

  long getTimestamp(long stepOffset);

  String getValue(long currentTimestamp, DB currentDb);

  String[] getValues(long currentTimestamp, DB currentDb);

  boolean hasValue(long currentTimestamp);

  long getInterval();

  void setInterval(long interval);

  String getDataType();

  void setDataType(String dataType);

  void setFrequency(int freq);

  String getTableName();

  Function getFunction();

  SensorGroup getSensorGroup();

  void setSensorGroup(SensorGroup sensorGroup);

  List<String> getFields();

  void setTick(long tick);
}
