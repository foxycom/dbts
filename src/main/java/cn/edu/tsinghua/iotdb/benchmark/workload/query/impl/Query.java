package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.enums.Aggregation;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;

import java.util.List;

public class Query {
    private List<Bike> bikes;
    private Sensor sensor;
    private Sensor gpsSensor;
    private long startTimestamp;
    private long endTimestamp;
    private Aggregation aggrFunc;
    private double threshold;

    public Query() { }

    public Query setBikes(List<Bike> bikes) {
        this.bikes = bikes;
        return this;
    }

    public Query setSensor(Sensor sensor) {
        this.sensor = sensor;
        return this;
    }

    public Query setGpsSensor(Sensor gpsSensor) {
        this.gpsSensor = gpsSensor;
        return this;
    }

    public Query setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
        return this;
    }

    public Query setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
        return this;
    }

    public Query setAggrFunc(Aggregation func) {
        this.aggrFunc = func;
        return this;
    }

    public Query setThreshold(double threshold) {
        this.threshold = threshold;
        return this;
    }

    public List<Bike> getBikes() {
        return bikes;
    }

    public Sensor getSensor() {
        return sensor;
    }

    public Sensor getGpsSensor() {
        return gpsSensor;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public Aggregation getAggrFunc() {
        return aggrFunc;
    }

    public double getThreshold() {
        return threshold;
    }
}
