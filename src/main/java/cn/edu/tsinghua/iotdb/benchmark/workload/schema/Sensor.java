package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;

import java.util.List;

public interface Sensor {
    String getName();

    long getTimestamp(long stepOffset);

    String getValue(long currentTimestamp);

    boolean hasValue(long currentTimestamp);

    long getInterval();

    void setInterval(long interval);

    String getDataType();

    void setDataType(String dataType);

    void setFrequency(int freq);

    Function getFunction();

    SensorGroup getSensorGroup();

    List<String> getFields();

}
