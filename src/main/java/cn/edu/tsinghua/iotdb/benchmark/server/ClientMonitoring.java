package cn.edu.tsinghua.iotdb.benchmark.server;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum ClientMonitoring {
    INSTANCE;

    private Config config = ConfigDescriptor.getInstance().getConfig();

    private Socket clientSocket;
    private PrintWriter out;
    private ObjectInputStream in;
    private Client client;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private MySqlLog mySqlLog = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
    private volatile State state = State.DEAD;
    private int countDown;
    private List<KPI> kpis;

    ClientMonitoring() {
        mySqlLog.initMysql(false);
        kpis = new ArrayList<>();
        countDown = config.CLIENT_NUMBER;
    }

    public void connect() {
        if (!config.MONITOR_SERVER) {
            return;
        }

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
        if (!config.MONITOR_SERVER) {
            return;
        }

        if (state == State.DEAD) {
            state = State.RUNNING;
            client = new Client(in);
            executor.submit(client);
            out.println(Message.START);
        }
    }

    public void stop() {
        if (!config.MONITOR_SERVER) {
            return;
        }

        if (state == State.RUNNING) {
            countDown--;
            System.out.println(String.format("%s is ready, remaining threads to wait: %d",
                    Thread.currentThread().getName(), countDown));
            if (countDown == 0) {
                countDown = config.CLIENT_NUMBER;
                out.println(Message.STOP);
                client.proceed = false;
                state = State.DEAD;
            }
        }
    }

    public void shutdown() {
        if (config.MONITOR_SERVER) {
            return;
        }

        if (state == State.RUNNING) {
            out.println(Message.STOP);
            client.proceed = false;
            state = State.DEAD;
            executor.shutdownNow();
        }
        for (KPI kpi : kpis) {
            mySqlLog.insertServerMetrics(kpi.getCpu(), kpi.getMem(), kpi.getSwap(), kpi.getIoWrites(),
                    kpi.getIoReads(), kpi.getNetRecv(), kpi.getNetTrans(), kpi.getDataSize());
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
                while (proceed && (kpi = (KPI) in.readObject()) != null) {
                    kpis.add(kpi);
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.err.println("Could not read KPI object from socket.");
            }
        }
    }

}
