package de.uni_passau.dbts.benchmark.client;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.measurement.Measurement;
import de.uni_passau.dbts.benchmark.tsdb.DBWrapper;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  protected static Config config = ConfigParser.INSTANCE.config();
  protected Measurement measurement;
  private CountDownLatch countDownLatch;
  protected CyclicBarrier barrier;
  int clientThreadId;
  DBWrapper dbWrapper;

  public Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    clientThreadId = id;
    measurement = new Measurement();
    dbWrapper = new DBWrapper(measurement);
  }

  public Measurement getMeasurement() {
    return measurement;
  }

  @Override
  public void run() {
    try {
      try {
        dbWrapper.init();
        barrier.await(); // wait for that all clients start test simultaneously
        doTest();
      } catch (Exception e) {
        LOGGER.error("Unexpected error: ", e);
      } finally {
        try {
          dbWrapper.close();
        } catch (TsdbException e) {
          LOGGER.error("Close {} error: ", config.DB_SWITCH, e);
        }
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  abstract void doTest();
}
