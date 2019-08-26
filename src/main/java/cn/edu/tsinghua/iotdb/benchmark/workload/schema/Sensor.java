package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;

public interface Sensor {
    String getName();

    long getTimestamp(long stepOffset);

    String getValue(long currentTimestamp);

    boolean hasValue(long currentTimestamp);

    long getInterval();

    void setInterval(long interval);

    String getTableName();

    String getDataType();

    void setDataType(String dataType);

    void setFrequency(int freq);

    FunctionParam getFunctionParam();

}
