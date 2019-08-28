package cn.edu.tsinghua.iotdb.benchmark.server;


import cn.edu.tsinghua.iotdb.benchmark.App;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.*;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
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

    private Config config = ConfigDescriptor.getInstance().getConfig();
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private ObjectOutputStream out;
    private BufferedReader in;
    private Monitor monitor;

    ServerMonitoring() {
        try {
            serverSocket = new ServerSocket(config.SERVER_MONITOR_PORT);
            System.out.println("Listening on port " + config.SERVER_MONITOR_PORT);
            listen();
        } catch (IOException e) {
            System.err.println("Could not create server socket at PORT " + config.SERVER_MONITOR_PORT);
        }
    }

    public void listen() throws IOException {
        while (true) {
            clientSocket = serverSocket.accept();
            out = new ObjectOutputStream(clientSocket.getOutputStream());
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            System.out.println("Monitor client connected.");
            monitor = new Monitor(out);

            String line;
            while ((line = in.readLine()) != null) {
                Message message = Message.valueOf(line.toUpperCase());
                switch (message) {
                    case START:
                        System.out.println("Starting monitoring.");
                        executor.submit(monitor);
                        break;
                    case PAUSE:
                        executor.shutdownNow();
                    case STOP:
                        monitor.stop();
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
        private boolean proceed = true;

        public Monitor(ObjectOutputStream out) {
            this.out = out;
        }

        @Override
        public void run() {
            /*float cpu = cpuUsage.get();
            Map<String, Float> memValues = memUsage.get();
            Map<String, Float> ioValues = ioUsage.get(config.DRIVE_NAME);
            Map<String, Float> netValues = netUsage.get(config.IFACE_NAME);

            float dataSize = 0.0f;
            try {
                dataSize = config.DB_SWITCH.getSize();
            } catch (TsdbException e) {
                LOGGER.error("Could not read the data size of {} because: {}", config.DB_SWITCH, e.getMessage());
            }*/
            float cpu = 2;
            Map<String, Float> memValues = new HashMap<>();
            memValues.put("memUsage", 34.0f);
            memValues.put("swapUsage", 23.0f);
            Map<String, Float> ioValues = new HashMap<>();
            ioValues.put("writesPerSec", 23f);
            ioValues.put("readsPerSec", 23f);
            Map<String, Float> netValues = new HashMap<>();
            netValues.put("transPerSec", 45f);
            netValues.put("recvPerSec", 23f);

            float dataSize = 23f;
            while (proceed) {
                KPI kpi = new KPI(cpu, memValues.get("memUsage"), memValues.get("swapUsage"), ioValues.get("writesPerSec"),
                        ioValues.get("readsPerSec"), netValues.get("recvPerSec"), netValues.get("transPerSec"), dataSize);
                try {
                    System.out.println("Sending object to socket");
                    out.writeObject(kpi);
                } catch (IOException e) {
                    proceed = false;
                    System.out.println("Could not write object.");
                }

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            try {
                out.writeObject(null);
            } catch (IOException e) {
                System.out.println("Cant write null");
            }
        }

        public synchronized void stop() {
            proceed = false;
        }
    }



    /*public void start(Config config) {
        File dir = new File(config.LOG_STOP_FLAG_PATH);

        boolean write2File = false;
        BufferedWriter out = null;
        char space = ' ';
        try {
            if (config.SERVER_MODE_INFO_FILE.length() > 0) {
                write2File = true;
                // if the file doesn't exits, then create the file, else append.
                out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(config.SERVER_MODE_INFO_FILE, true)));
                out.write(String.format("Time%15cCPUUsage%7cRAM%5cDiskIO%5ceth0In%5ceth0Out%5cTotalFiles%5cDataAndWalFiles%5cSockets"
                                + "%5cdeltaFileNum%5cderbyFileNum%5cdigestFileNum%5cmetadataFileNum%5coverflowFileNum%5cwalsFileNum\r\n",
                        space, space, space, space, space, space, space, space));
            }

            if (dir.exists() && dir.isDirectory()) {
                File file = new File(config.LOG_STOP_FLAG_PATH + "/log_stop_flag");
                int interval = config.INTERVAL;
                LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
                while (true) {

                    *//*float cpu = cpuUsage.get();
                    Map<String, Float> memValues = memUsage.get();
                    Map<String, Float> ioValues = ioUsage.get(config.DRIVE_NAME);
                    Map<String, Float> netValues = netUsage.get(config.IFACE_NAME);

                    float dataSize = 0.0f;
                    try {
                        dataSize = config.DB_SWITCH.getSize();
                    } catch (TsdbException e) {
                        LOGGER.error("Could not read the data size of {} because: {}", config.DB_SWITCH, e.getMessage());
                    }*//*

                    *//*mySql.insertServerMetrics(cpu, memValues.get("memUsage"), memValues.get("swapUsage"),
                            ioValues.get("writesPerSec"), ioValues.get("readsPerSec"), netValues.get("recvPerSec"),
                            netValues.get("transPerSec"), dataSize);
                    String log = "CPU: %f %% | RAM: %f %%  | Swap: %f %% | IO writes: %f/s | IO reads: %f/s | Net Recv: %f KB/s | Net Trans: %f KB/s | Size: %f";*//*
                    *//*System.out.println(String.format(Locale.US, log, cpu, memValues.get("memUsage"), memValues.get("swapUsage"),
                            ioValues.get("writesPerSec"), ioValues.get("readsPerSec"), netValues.get("recvPerSec"), netValues.get("transPerSec"), dataSize));*//*

                    *//*if (write2File) {
                        out.write(String.format("%d%14f%14f%15f", System.currentTimeMillis(),
                                ioUsageList.get(0), MemUsage.getInstance().get(), ioUsageList.get(1)));
                        out.write(String.format("%16f%16f%12d%8s%8d%10s%5d", netUsageList.get(0),
                                netUsageList.get(1), openFileList.get(0), space, openFileList.get(1),
                                space, openFileList.get(2)));
                        out.write(String.format("%16d%16d%16d%16d%16d%16d\n", openFileList.get(3),
                                openFileList.get(4), openFileList.get(5), space, openFileList.get(6),
                                openFileList.get(7), openFileList.get(8)));
                    }*//*

                    try {
                        Thread.sleep(interval * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (file.exists()) {
                        boolean f = file.delete();
                        if (!f) {
                            LOGGER.error("log_stop_flag 文件删除失败");
                        }
                        break;
                    }
                }
            } else {
                LOGGER.error("LOG_STOP_FLAG_PATH not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            mySql.closeMysql();
            try {
                if (out != null)
                    out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }*/
}
