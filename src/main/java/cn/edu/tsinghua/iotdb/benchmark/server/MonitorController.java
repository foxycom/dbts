package cn.edu.tsinghua.iotdb.benchmark.server;

public class MonitorController implements Runnable {
    private ClientMonitoring clientMonitoring = ClientMonitoring.INSTANCE;

    @Override
    public void run() {
        System.err.println(".run() method called");
        System.out.println("Stopped client monitoring from MonitorController");
        clientMonitoring.stop();
    }
}
