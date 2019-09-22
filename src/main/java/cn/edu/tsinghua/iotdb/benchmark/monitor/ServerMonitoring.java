package cn.edu.tsinghua.iotdb.benchmark.monitor;

import cn.edu.tsinghua.iotdb.benchmark.App;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.*;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum ServerMonitoring {
  INSTANCE;

  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
  private CpuUsage cpuUsage = CpuUsage.INSTANCE;
  private MemUsage memUsage = MemUsage.INSTANCE;
  private IoUsage ioUsage = IoUsage.INSTANCE;
  private NetUsage netUsage = NetUsage.INSTANCE;

  private ExecutorService executor = Executors.newSingleThreadExecutor();
  private ServerSocket serverSocket;
  private Socket clientSocket;
  private ObjectOutputStream out;
  private BufferedReader in;
  private Monitor monitor;
  private Config config;

  ServerMonitoring() {}

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

  private class Monitor implements Runnable {
    private ObjectOutputStream out;
    private volatile boolean proceed = true;

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
