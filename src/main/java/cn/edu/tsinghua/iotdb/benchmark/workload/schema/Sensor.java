package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;

import static cn.edu.tsinghua.iotdb.benchmark.conf.Constants.START_TIMESTAMP;

public class Sensor {
    private String name;
    private FunctionParam functionParam;
    private int freq;

    public Sensor(String name, FunctionParam functionParam) {
        this(name, functionParam, 1);
    }

    public Sensor(String name, FunctionParam functionParam, int freq) {
        this.name = name;
        this.functionParam = functionParam;
        this.freq = freq;
    }

    public String getName() {
        return name;
    }

    public FunctionParam getFunctionParam() {
        return functionParam;
    }

    public Number getValue(long currentTimestamp) {
        return Function.getValueByFunctionIdAndParam(functionParam, currentTimestamp);
    }

    public boolean hasValue(long currentTimestamp) {
        long startTimestamp = START_TIMESTAMP;

        return true;
    }
}
