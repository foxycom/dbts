package de.uni_passau.dbts.benchmark.monitor;

import de.uni_passau.dbts.benchmark.App;
import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.sersyslog.CpuUsage;
import de.uni_passau.dbts.benchmark.sersyslog.IoUsage;
import de.uni_passau.dbts.benchmark.sersyslog.MemUsage;
import de.uni_passau.dbts.benchmark.sersyslog.NetUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The system monitoring server.
 */
public enum MonitoringServer {
  INSTANCE;

  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

  /** CPU usage reader. */
  private CpuUsage cpuUsage = CpuUsage.INSTANCE;

  /** Memory and swap usage reader. */
  private MemUsage memUsage = MemUsage.INSTANCE;

  /** IO usage reader. */
  private IoUsage ioUsage = IoUsage.INSTANCE;

  /** Network usage reader. */
  private NetUsage netUsage = NetUsage.INSTANCE;

  /** Executor service of the worker thread that communicates with the monitoring client. */
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  /** Socket. */
  private ServerSocket serverSocket;

  /** Client's socket. */
  private Socket clientSocket;

  /** Output. */
  private ObjectOutputStream out;

  /** Input. */
  private BufferedReader in;

  /** Worker thread. */
  private Monitor monitor;

  /** Configuration singleton. */
  private Config config;

  /** Creates a new instance. */
  MonitoringServer() {}

  /** Listens for new connections. When the monitoring client connects, waits for messages that
   * determine what to do next.
   *
   * @param config Config instance.
   * @throws IOException if a socket error occurs.
   */
  public void listen(Config config) throws IOException {
    this.config = config;
    serverSocket = new ServerSocket(config.SERVER_MONITOR_PORT);
    System.out.println("Listening on port " + config.SERVER_MONITOR_PORT);

    while (true) {
      clientSocket = serverSocket.accept();
      out = new ObjectOutputStream(clientSocket.getOutputStream());
      in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      System.out.println("Monitor client connected.");
      monitor = new Monitor(out);

      String line;
      while (!clientSocket.isClosed() && (line = in.readLine()) != null) {
        Message message = Message.valueOf(line.toUpperCase());
        switch (message) {
          case START:
            System.out.println("Starting monitoring.");
            monitor.proceed = true;
            executor.submit(monitor);
            break;
          case CLOSE:
            System.out.println("closing socket");
            monitor.proceed = false;
            clientSocket.close();
            break;
          case STOP:
            monitor.proceed = false;
            System.out.println("Stopping monitoring.");
            break;
          default:
            System.err.println("Unknown message.");
        }
      }
    }
  }

  /** Worker thread that reads system KPIs communicates with the monitoring client. */
  private class Monitor implements Runnable {
    private ObjectOutputStream out;

    /** Proceed reading system KPIs. */
    private volatile boolean proceed = true;

    /**
     * Creates a new instance.
     *
     * @param out Output stream.
     */
    public Monitor(ObjectOutputStream out) {
      this.out = out;
    }

    @Override
    public void run() {
      while (proceed) {
        long st = System.nanoTime();
        float cpu = cpuUsage.get();
        Map<String, Float> memValues = memUsage.get();
        Map<String, Float> ioValues = ioUsage.get(config.DRIVE_NAME);
        Map<String, Float> netValues = netUsage.get(config.IFACE_NAME);
        float dataSize = 0.0f;
        try {
          dataSize = config.DB_SWITCH.getSize();
        } catch (TsdbException e) {
          LOGGER.error(
              "Could not read the data size of {} because: {}", config.DB_SWITCH, e.getMessage());
        }
        KPI kpi =
            new KPI(
                cpu,
                memValues.get("memUsage"),
                memValues.get("swapUsage"),
                ioValues.get("writesPerSec"),
                ioValues.get("readsPerSec"),
                netValues.get("recvPerSec"),
                netValues.get("transPerSec"),
                dataSize);
        try {
          if (proceed) {
            System.out.println("Sending object to socket.");
            out.writeObject(kpi);
          } else {
            break;
          }
        } catch (IOException e) {
          proceed = false;
          System.out.println("Could not write object.");
        }

        long en = System.nanoTime();
        System.out.println("KPI took: " + ((en - st) / Constants.NANO_TO_MILLIS) + " ms");
      }
    }
  }
}
