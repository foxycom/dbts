package de.uni_passau.dbts.benchmark.client;

import de.uni_passau.dbts.benchmark.workload.SyntheticWorkload;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.slf4j.LoggerFactory;

/**
 * This client type is used to generate synthetic workload on the benchmarked databases; hence,
 * it exploits the {@link SyntheticWorkload} implementation.
 */
public class SyntheticClient extends BaseClient {

  /**
   * Creates a worker client instance.
   *
   * @param id The client's ID.
   * @param countDownLatch Down latch.
   * @param barrier A barrier the client should be bound to.
   */
  public SyntheticClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier, new SyntheticWorkload(id));
  }

  @Override
  void initLogger() {
    LOGGER = LoggerFactory.getLogger(SyntheticClient.class);
  }
}
