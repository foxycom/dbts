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

/**
 * This class defines the base behavior of all worker clients. Each client is bound by a cyclic
 * barrier, which a the client implementation should call when its execution finished. Thus, the
 * clients' executions can be synchronized.
 */
public abstract class Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  /** Config singleton. */
  protected static Config config = ConfigParser.INSTANCE.config();

  /** Measurement instance that the client can write execution results into. */
  protected Measurement measurement;

  /** Down latch. */
  private CountDownLatch countDownLatch;

  /** The barrier which is used to synchronize worker clients. */
  CyclicBarrier barrier;

  /** ID of the client. */
  int clientThreadId;

  /** Database connection wrapper. */
  DBWrapper dbWrapper;

  /**
   * Creates an instance of worker client.
   *
   * @param id ID of the client.
   * @param countDownLatch Down latch object.
   * @param barrier A barrier the client should be bound to.
   */
  public Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    clientThreadId = id;
    measurement = new Measurement();
    dbWrapper = new DBWrapper(measurement);
  }

  /**
   * Returns the measurement results of the client.
   *
   * @return Measurement results.
   */
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
          LOGGER.error("Error while closing connection to {}: ", config.DB_SWITCH, e);
        }
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  /**
   * Starts worker's operations.
   */
  abstract void doTest();
}
