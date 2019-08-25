package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;

import java.util.Objects;

import static cn.edu.tsinghua.iotdb.benchmark.conf.Constants.START_TIMESTAMP;

public class Sensor {


    private String name;
    private FunctionParam functionParam;
    private int interval;
    private String dataType;


    public Sensor(String name, FunctionParam functionParam) {
        this(name, functionParam, 1);
    }

    /**
     * Creates an instance of sensor.
     *
     * @param name The name of the sensor.
     * @param functionParam The parameters of data function.
     * @param freq The frequency the sensor samples its data (in Hz).
     */
    public Sensor(String name, FunctionParam functionParam, int freq) {
        this.name = name;
        this.functionParam = functionParam;
        this.interval = 1000 / freq;    // in ms

    }

    public String getName() {
        return name;
    }

    public FunctionParam getFunctionParam() {
        return functionParam;
    }

    public long getTimestamp(long stepOffset) {
        return START_TIMESTAMP + interval * stepOffset;
    }

    public Number getValue(long currentTimestamp) {
        return Function.getValueByFunctionIdAndParam(functionParam, currentTimestamp);
    }

    public boolean hasValue(long currentTimestamp) {
        long timeDelta = currentTimestamp - START_TIMESTAMP;
        assert timeDelta >= 0;

        return timeDelta % interval == 0;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public String getTableName() {
        return name + "_series";
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setFrequency(int frequency) {
        this.interval = 1000 / frequency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sensor sensor = (Sensor) o;
        return interval == sensor.interval &&
                name.equals(sensor.name) &&
                functionParam.equals(sensor.functionParam);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, functionParam, interval);
    }
}
