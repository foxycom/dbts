package cn.edu.tsinghua.iotdb.benchmark.server;


import java.io.Serializable;

public class KPI implements Serializable {
    private float cpu;
    private float mem;
    private float swap;
    private float ioWrites;
    private float ioReads;
    private float netRecv;
    private float netTrans;
    private float dataSize;

    public KPI(float cpu, float mem, float swap, float ioWrites, float ioReads, float netRecv, float netTrans,
               float dataSize) {
        this.cpu = cpu;
        this.mem = mem;
        this.swap = swap;
        this.ioWrites = ioWrites;
        this.ioReads = ioReads;
        this.netRecv = netRecv;
        this.netTrans = netTrans;
        this.dataSize = dataSize;
    }

    public double getCpu() {
        return cpu;
    }

    public double getMem() {
        return mem;
    }

    public double getSwap() {
        return swap;
    }

    public double getIoWrites() {
        return ioWrites;
    }

    public double getIoReads() {
        return ioReads;
    }

    public double getNetRecv() {
        return netRecv;
    }

    public double getNetTrans() {
        return netTrans;
    }

    public double getDataSize() {
        return dataSize;
    }
}
