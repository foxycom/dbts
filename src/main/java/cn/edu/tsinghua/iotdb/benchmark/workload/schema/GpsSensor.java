package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;

public class GpsSensor extends BasicSensor {

    public GpsSensor(String name, FunctionParam functionParam, int freq) {
        super(name, functionParam, freq);
    }

    @Override
    public String getValue(long currentTimestamp) {
        // FIXME cycle is static
        GeoPoint geoPoint = Function.getGeoLinePoint(functionParam.getMax(), functionParam.getMin(),
                functionParam.getCycle(), currentTimestamp);
        return "'" + geoPoint.toString() + "'";
    }
}
