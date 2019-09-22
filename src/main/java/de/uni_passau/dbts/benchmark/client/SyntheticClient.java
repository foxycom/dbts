package de.uni_passau.dbts.benchmark.client;

import de.uni_passau.dbts.benchmark.workload.SyntheticWorkload;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.slf4j.LoggerFactory;

public class SyntheticClient extends BaseClient {

  public SyntheticClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier, new SyntheticWorkload(id));
  }

  @Override
  void initLogger() {
    LOGGER = LoggerFactory.getLogger(SyntheticClient.class);
  }
}
