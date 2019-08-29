package cn.edu.tsinghua.iotdb.benchmark.monitor;

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
            state = State.STOPPED;
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

        if (state == State.STOPPED) {
            state = State.RUNNING;
            client = new Client(in);
            executor.submit(client);
            out.println(Message.START);
        }
    }

    private void sendStop() {
        out.println(Message.STOP);
        client.proceed = false;
        state = State.STOPPED;
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
                sendStop();
            }
        }
    }

    public void shutdown() {
        if (!config.MONITOR_SERVER) {
            return;
        }

        if (state != state.DEAD) {
            out.println(Message.CLOSE);
            client.proceed = false;
            state = State.DEAD;
            executor.shutdownNow();
        }

        mySqlLog.insertServerMetrics(kpis);
    }

    private enum State {
        RUNNING,
        STOPPED,
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
