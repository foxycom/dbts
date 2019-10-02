package de.uni_passau.dbts.benchmark.monitor;

import de.uni_passau.dbts.benchmark.App;
import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.mysql.MySqlLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A client that connects to a system monitor (see {@link MonitoringServer}) over TCP and saves the
 * KPI data that the server collects to the MySQL database.
 */
public enum MonitoringClient {
  INSTANCE;

  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

  /** Configuration singleton. */
  private Config config = ConfigParser.INSTANCE.config();

  /** Output writer. */
  private PrintWriter out;

  /** Input reader. */
  private ObjectInputStream in;

  /** Client thread. */
  private Client client;

  /** Executor service for the client thread. */
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  /** The MySQL logger instance. */
  private MySqlLog mySqlLog = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);

  /** State of the connection. */
  private volatile State state = State.DEAD;

  /** */
  private int countDown;

  /** List of the system KPIs collected by {@link MonitoringServer}. */
  private List<KPI> kpis;

  /**
   * Creates a new client.
   */
  MonitoringClient() {
    mySqlLog.initMysql(false);
    kpis = new ArrayList<>();
    countDown = config.CLIENTS_NUMBER;
  }

  /**
   * Connects to a system monitor.
   */
  public void connect() {
    if (!config.MONITOR_SERVER) {
      return;
    }

    try {
      Socket clientSocket = new Socket(config.HOST, config.SERVER_MONITOR_PORT);
      out = new PrintWriter(clientSocket.getOutputStream(), true);
      in = new ObjectInputStream(clientSocket.getInputStream());
      state = State.STOPPED;
      client = new Client(in);

    } catch (IOException e) {
      System.err.println("Could not connect to server monitor!");
      System.exit(1);
    }
  }

  /**
   * Starts a client thread that collects the KPIs from the monitoring server.
   */
  public void start() {
    if (!config.MONITOR_SERVER) {
      return;
    }

    if (state == State.STOPPED) {
      state = State.RUNNING;
      client = new Client(in);
      executor.submit(client);
      out.println(Message.START);
    }
  }

  /**
   * Signals the monitoring server to stop reading system KPIs.
   */
  private void sendStop() {
    out.println(Message.STOP);
    client.proceed = false;
    state = State.STOPPED;
  }

  /**
   * Called by a benchmark worker thread, signals that a worker thread has finished an operation.
   * When all worker threads are done, the client sends a message the monitoring server to stop
   * reading system KPIs.
   */
  public void stop() {
    if (!config.MONITOR_SERVER) {
      return;
    }

    if (state == State.RUNNING) {
      countDown--;
      LOGGER.info(
          "{} is ready, remaining threads to wait: {}",
          Thread.currentThread().getName(),
          countDown);
      if (countDown == 0) {
        countDown = config.CLIENTS_NUMBER;
        sendStop();
      }
    }
  }

  /**
   * Stops the monitoring client and saves the results into the MySQL database.
   */
  public void shutdown() {
    if (!config.MONITOR_SERVER) {
      return;
    }

    if (state != State.DEAD) {
      out.println(Message.CLOSE);
      client.proceed = false;
      state = State.DEAD;
      executor.shutdownNow();
    }

    mySqlLog.saveServerMetrics(kpis);
  }

  /**
   * States of the monitoring client.
   */
  private enum State {
    RUNNING,
    STOPPED,
    DEAD
  }

  /**
   * Client thread that directly communicates with the monitoring server.
   */
  private class Client implements Runnable {
    private ObjectInputStream in;

    /** Proceed listening for new KPIs. */
    private volatile boolean proceed = true;

    /**
     * Creates a new instance.
     *
     * @param in Input reader.
     */
    public Client(ObjectInputStream in) {
      this.in = in;
    }

    @Override
    public void run() {
      KPI kpi;
      try {
        while (proceed && (kpi = (KPI) in.readObject()) != null) {
          kpis.add(kpi);
          if (kpis.size() > 20) {
            mySqlLog.saveServerMetrics(kpis);
            kpis.clear();
          }
        }
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        System.err.println("Could not read KPI object from socket.");
      }
    }
  }
}
