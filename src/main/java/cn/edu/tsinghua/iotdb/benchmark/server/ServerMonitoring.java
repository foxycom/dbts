package cn.edu.tsinghua.iotdb.benchmark.server;


import cn.edu.tsinghua.iotdb.benchmark.App;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.*;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public enum ServerMonitoring {
    INSTANCE;

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private CpuUsage cpuUsage = CpuUsage.INSTANCE;
    private MemUsage memUsage = MemUsage.INSTANCE;
    private IoUsage ioUsage = IoUsage.INSTANCE;
    private NetUsage netUsage = NetUsage.INSTANCE;

    public void start(Config config) {
        MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
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
                // 检测所需的时间在目前代码的参数下至少为2秒
                LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
                while (true) {
                    /*ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
                    ArrayList<Float> netUsageList = NetUsage.getInstance().get();
                    ArrayList<Integer> openFileList = OpenFileNumber.getInstance().get();
                    fileSizeStatistics = FileSize.getInstance().getFileSize();
                    ioStatistics = IoUsage.getInstance().getIOStatistics();
                    LOGGER.info("CPU Usage,{}", ioUsageList.get(0));
                    LOGGER.info("RAM Usage,{}", MemUsage.getInstance().get());
                    LOGGER.info("Disk usage (GB),{}", MemUsage.getInstance().getProcessMemUsage());
                    LOGGER.info("Disk IO: {}, TPS: {}, Read speed MB/s: {}, Write speed MB/s: {}",
                            ioUsageList.get(1),
                            ioStatistics.get(IoUsage.IOStatistics.TPS),
                            ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                            ioStatistics.get(IoUsage.IOStatistics.MB_WRTN));
                    LOGGER.info("eth0 In Out: {} | {},KB/s", netUsageList.get(0), netUsageList.get(1));
                    LOGGER.info("PID={}, overall files opened: {}, files opened in path: {}, irgendwas :{}", OpenFileNumber.getInstance().getPid(),
                            openFileList.get(0), openFileList.get(1), openFileList.get(2));
                    LOGGER.info("Data size GB, data: {}, info: {}, metadata: {}, overflow: {}, delta: {}, wal: {}",
                            fileSizeStatistics.get(FileSize.FileSizeKinds.DATA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.INFO),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.METADATA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.OVERFLOW),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.DELTA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.WAL));*/
                    float cpu = cpuUsage.get();
                    Map<String, Float> memValues = memUsage.get();
                    Map<String, Float> ioValues = ioUsage.get(config.DRIVE_NAME);
                    Map<String, Float> netValues = netUsage.get(config.IFACE_NAME);

                    float dataSize = 0.0f;
                    try {
                        dataSize = config.DB_SWITCH.getSize();
                    } catch (TsdbException e) {
                        LOGGER.error("Could not read the data size of {} because: {}", config.DB_SWITCH, e.getMessage());
                    }

                    /*mySql.insertServerMetrics(cpu, memValues.get("memUsage"), memValues.get("swapUsage"),
                            ioValues.get("writesPerSec"), ioValues.get("readsPerSec"), netValues.get("recvPerSec"),
                            netValues.get("transPerSec"), dataSize);*/
                    String log = "CPU: %f% | RAM: %f%  | Swap: %f% | IO writes: %f/s | IO reads: %f/s | Net Recv: %f KB/s | Net Trans: %f KB/s | Size: %f";
                    System.out.println(String.format(Locale.US, log, cpu, memValues.get("memUsage"), memValues.get("swapUsage"),
                            ioValues.get("writesPerSec"), ioValues.get("readsPerSec"), netValues.get("recvPerSec"), netValues.get("transPerSec"), dataSize));

                    /*if (write2File) {
                        out.write(String.format("%d%14f%14f%15f", System.currentTimeMillis(),
                                ioUsageList.get(0), MemUsage.getInstance().get(), ioUsageList.get(1)));
                        out.write(String.format("%16f%16f%12d%8s%8d%10s%5d", netUsageList.get(0),
                                netUsageList.get(1), openFileList.get(0), space, openFileList.get(1),
                                space, openFileList.get(2)));
                        out.write(String.format("%16d%16d%16d%16d%16d%16d\n", openFileList.get(3),
                                openFileList.get(4), openFileList.get(5), space, openFileList.get(6),
                                openFileList.get(7), openFileList.get(8)));
                    }*/

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
    }
}
