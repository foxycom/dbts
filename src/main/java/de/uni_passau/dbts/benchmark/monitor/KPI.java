package de.uni_passau.dbts.benchmark.monitor;

import java.io.Serializable;

/**
 * A container holding a single reading of system KPIs.
 */
public class KPI implements Serializable {
  private float cpu;
  private float mem;
  private float swap;
  private float ioWrites;
  private float ioReads;
  private float netRecv;
  private float netTrans;
  private float dataSize;
  private long timestamp;

  /**
   * Creates a new instance.
   *
   * @param cpu CPU utilization in %.
   * @param mem Memory usage in %.
   * @param swap Swap usage in %.
   * @param ioWrites Number of write operations per second.
   * @param ioReads Number of read operations per second.
   * @param netRecv Network input in KB/s.
   * @param netTrans Network output in KB/s.
   * @param dataSize Disk usage in %.
   */
  public KPI(
      float cpu,
      float mem,
      float swap,
      float ioWrites,
      float ioReads,
      float netRecv,
      float netTrans,
      float dataSize) {
    this.timestamp = System.currentTimeMillis();
    this.cpu = cpu;
    this.mem = mem;
    this.swap = swap;
    this.ioWrites = ioWrites;
    this.ioReads = ioReads;
    this.netRecv = netRecv;
    this.netTrans = netTrans;
    this.dataSize = dataSize;
  }

  /**
   * Returns the timestamp of the reading.
   *
   * @return Timestamp.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Returns the CPU utilization.
   *
   * @return CPU utilization in %.
   */
  public double getCpu() {
    return cpu;
  }

  /**
   * Returns the memory usage.
   *
   * @return Memory usage in %.
   */
  public double getMem() {
    return mem;
  }

  /**
   * Returns the swap usage.
   *
   * @return Swap usage in %.
   */
  public double getSwap() {
    return swap;
  }

  /**
   * Returns the number of write operations.
   *
   * @return Number of write operations per second.
   */
  public double getIoWrites() {
    return ioWrites;
  }

  /**
   * Returns the number of read operations.
   *
   * @return Number of read operations per second.
   */
  public double getIoReads() {
    return ioReads;
  }

  /**
   * Returns the network input.
   *
   * @return Network input in KB/s.
   */
  public double getNetRecv() {
    return netRecv;
  }

  /**
   * Returns the network output.
   *
   * @return Network output in KB/s.
   */
  public double getNetTrans() {
    return netTrans;
  }

  /**
   * Returns disk usage.
   *
   * @return Disk usage in %.
   */
  public double getDataSize() {
    return dataSize;
  }

  @Override
  public String toString() {
    return "KPI{"
        + "cpu="
        + cpu
        + ", mem="
        + mem
        + ", swap="
        + swap
        + ", ioWrites="
        + ioWrites
        + ", ioReads="
        + ioReads
        + ", netRecv="
        + netRecv
        + ", netTrans="
        + netTrans
        + ", dataSize="
        + dataSize
        + '}';
  }
}
