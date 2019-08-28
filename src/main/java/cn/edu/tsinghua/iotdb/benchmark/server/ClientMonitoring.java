package cn.edu.tsinghua.iotdb.benchmark.server;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum ClientMonitoring {
    INSTANCE;

    private static Config config = ConfigDescriptor.getInstance().getConfig();

    private Socket clientSocket;
    private PrintWriter out;
    private ObjectInputStream in;
    private Client client;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private static MySqlLog mySqlLog = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
    private volatile State state = State.DEAD;

    public void connect() {
        try {
            clientSocket = new Socket(config.HOST, config.SERVER_MONITOR_PORT);
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new ObjectInputStream(clientSocket.getInputStream());
            client = new Client(in);

        } catch (IOException e) {
            System.err.println("Could not connect to server monitor!");
            System.exit(1);
        }
    }

    public void start() {
        if (state == State.DEAD) {
            state = State.RUNNING;
            executor.submit(client);
            out.println(Message.START);
        } else {
            System.err.println(".start(), but monitor is already running");

        }
    }

    public void stop() {
        if (state == State.RUNNING) {
            out.println(Message.STOP);
            client.proceed = false;
            executor.shutdownNow();
            state = State.DEAD;
        } else {
            System.err.println(".stop(), but monitor is already dead");
        }
    }

    private enum State {
        RUNNING,
        DEAD;
    }

    private class Client implements Runnable {
        private ObjectInputStream in;
        private volatile boolean proceed = true;

        public Client(ObjectInputStream in) {
            this.in = in;
        }

        @Override
        public void run() {
            KPI kpi;
            try {
                System.out.println("Start reading.");
                while (proceed && (kpi = (KPI) in.readObject()) != null) {
                    mySqlLog.insertServerMetrics(kpi.getCpu(), kpi.getMem(), kpi.getSwap(), kpi.getIoWrites(),
                            kpi.getIoReads(), kpi.getNetRecv(), kpi.getNetTrans(), kpi.getDataSize());
                }
                System.out.println("Stop reading.");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.err.println("Could not read KPI object from socket.");
            }
            System.out.println("end of run()");
        }
    }

}
