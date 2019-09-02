package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;

public class GpsSensor extends BasicSensor {

    public GpsSensor(String name, SensorGroup sensorGroup, FunctionParam functionParam, int freq, String dataType) {
        super(name, sensorGroup, functionParam, freq, dataType);
    }

    @Override
    public String getValue(long currentTimestamp) {
        // FIXME cycle is static
        GeoPoint geoPoint = Function.getGeoLinePoint(functionParam.getMax(), functionParam.getMin(),
                functionParam.getCycle(), currentTimestamp);
        return geoPoint.toString();
    }
}
