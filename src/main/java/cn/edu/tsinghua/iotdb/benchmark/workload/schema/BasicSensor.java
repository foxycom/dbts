package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;

import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Objects;

import static cn.edu.tsinghua.iotdb.benchmark.conf.Constants.START_TIMESTAMP;

public class BasicSensor implements Sensor {

    private static NumberFormat nf = NumberFormat.getNumberInstance(Locale.US);

    protected FunctionParam functionParam;

    private String name;
    private String sensorGroup;
    private long interval;
    private String dataType;

    public BasicSensor(String name, FunctionParam param) {
    }

    /**
     * Creates an instance of sensor.
     *
     * @param name The name of the sensor.
     * @param functionParam The parameters of data function.
     * @param freq The frequency the sensor samples its data (in Hz).
     */
    public BasicSensor(String name, String sensorGroup, FunctionParam functionParam, int freq, String dataType) {
        nf.setRoundingMode(RoundingMode.HALF_UP);
        nf.setMaximumFractionDigits(2);
        nf.setMinimumFractionDigits(2);
        nf.setGroupingUsed(false);

        this.name = name;
        this.sensorGroup = sensorGroup;
        this.functionParam = functionParam;
        this.interval = 1000 / freq;    // in ms
        this.dataType = dataType;

    }

    public String getName() {
        return name;
    }

    public long getTimestamp(long stepOffset) {
        return START_TIMESTAMP + interval * stepOffset;
    }

    public String getValue(long currentTimestamp) {
        Number value = Function.getValueByFunctionIdAndParam(functionParam, currentTimestamp);
        String convertedValue = nf.format(value);
        return convertedValue;
    }

    public boolean hasValue(long currentTimestamp) {
        // TODO implement
        return true;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
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
    public FunctionParam getFunctionParam() {
        return functionParam;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sensor sensor = (Sensor) o;
        return interval == sensor.getInterval() &&
                name.equals(sensor.getName()) &&
                functionParam.equals(sensor.getFunctionParam());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, functionParam, interval);
    }
}
