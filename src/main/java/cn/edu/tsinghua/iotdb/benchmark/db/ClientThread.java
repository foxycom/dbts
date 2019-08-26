package cn.edu.tsinghua.iotdb.benchmark.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;

public class ClientThread implements Runnable{
	private IDatebase database;
	private MySqlLog mySql;
	private int index;
	private Config config;
	private static final Logger LOGGER = LoggerFactory.getLogger(ClientThread.class);
	private Storage storage;
	private static ThreadLocal<Long> totalTime = new ThreadLocal<Long>(){
		protected Long initialValue(){
			return  (long) 0;
		}
	};
	private static ThreadLocal<Long> errorCount = new ThreadLocal<Long>(){
		protected Long initialValue(){
			return  (long) 0;
		}
	};
	private CountDownLatch downLatch;
	private ArrayList<Long> totalTimes;
	private ArrayList<Long> totalInsertErrorNums;
	private ArrayList<Long> latencies;
	private ArrayList<ArrayList> latenciesOfClients;

	public ClientThread(IDatebase datebase, int index, CountDownLatch downLatch,
			ArrayList<Long> totalTimes, ArrayList<Long> totalInsertErrorNums, ArrayList<ArrayList> latenciesOfClients) {
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.database = datebase;
		this.index = index;
		this.downLatch = downLatch;
		this.totalTimes = totalTimes;
		this.totalInsertErrorNums = totalInsertErrorNums;
		this.latencies = new ArrayList<>();
		this.latenciesOfClients = latenciesOfClients;
		mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
		mySql.initMysql(false);
	}

	public ClientThread(IDatebase datebase, int index , Storage storage, CountDownLatch downLatch,
			ArrayList<Long> totalTimes, ArrayList<Long> totalInsertErrorNums, ArrayList<ArrayList> latenciesOfClients) {
		this.config = ConfigDescriptor.getInstance().getConfig();
		this.database = datebase;
		this.index = index;
		this.storage = storage;
		this.downLatch = downLatch;
		this.totalTimes = totalTimes;
		this.totalInsertErrorNums = totalInsertErrorNums;
		this.latencies = new ArrayList<>();
		this.latenciesOfClients = latenciesOfClients;
		mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
		mySql.initMysql(false);
	}


	@Override
	public void run() {
		int i = 0;
//		try {
//			database.init();
//		} catch (SQLException e1) {
//			LOOGER.error("{} Fail to init database becasue {}",Thread.currentThread().getName(), e1.getMessage());
//			return;
//		}

		if(config.READ_FROM_FILE){
			while(true){
				try {
					LinkedList<String> cons = storage.consume(config.BATCH_OP_NUM);
					//线程结束
					if(cons.size() == 0) {
						break;
					}
					database.insertOneBatch(cons , i, totalTime, errorCount, latencies);
				} catch (SQLException e) {
					LOGGER.error("{} Fail to insert one batch into database becasue {}",Thread.currentThread().getName(), e.getMessage());
				}
				i++;
			}
		}
		else{
			int clientDevicesNum = config.DEVICE_NUMBER/config.CLIENT_NUMBER;
			LinkedList<String> deviceCodes = new LinkedList<>();
			//may not correct in multiple device per batch mode
			long pointsOneLoop = config.DEVICE_NUMBER / config.CLIENT_NUMBER * config.SENSOR_NUMBER * config.BATCH_SIZE;
			double actualLoopSecond = (double) pointsOneLoop / config.CLIENT_MAX_WRT_RATE;
			//overflow mode 2 related variables initial
			Random random = new Random(config.QUERY_SEED);
			ArrayList<Integer> before = new ArrayList<>();
			int maxIndex = (int) (config.BATCH_SIZE * config.LOOP * config.OVERFLOW_RATIO);
			int currMaxIndexOfDist = config.START_TIMESTAMP_INDEX;
			if(config.IS_OVERFLOW && config.OVERFLOW_MODE==1) {
				for (int beforeIndex = 0; beforeIndex < maxIndex; beforeIndex++) {
					before.add(beforeIndex);
				}
			}
			for (int m = 0; m < clientDevicesNum; m++) {
				deviceCodes.add(config.DEVICE_CODES.get(index * clientDevicesNum + m));
			}
			while(i < config.LOOP) {
				long oldTotalTime = totalTime.get();
				if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)) {
					try {
						database.insertGenDataOneBatch(config.STORAGE_GROUP_NAME + "." + config.TIMESERIES_NAME, i, totalTime, errorCount, latencies);
					} catch (SQLException e) {
						LOGGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				} else if (config.MUL_DEV_BATCH && !config.IS_OVERFLOW) {
					try {
						database.insertOneBatchMulDevice(deviceCodes, i, totalTime, errorCount, latencies);
					} catch (SQLException e) {
						LOGGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				} else if (!config.IS_OVERFLOW) {
					try {
						for (int m = 0; m < clientDevicesNum; m++) {
							database.insertOneBatch(config.DEVICE_CODES.get(index * clientDevicesNum + m), i, totalTime, errorCount, latencies);
						}
					} catch (SQLException e) {
						LOGGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				} else if (config.OVERFLOW_MODE == 1) {
					try {
						for (int m = 0; m < clientDevicesNum; m++) {
							maxIndex = database.insertOverflowOneBatch(config.DEVICE_CODES.get(index * clientDevicesNum + m),
									i,
									totalTime,
									errorCount,
									before,
									maxIndex,
									random, latencies);
						}
					} catch (SQLException e) {
						LOGGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				} else if (config.OVERFLOW_MODE == 2) {
					try {
						for (int m = 0; m < clientDevicesNum; m++) {
							currMaxIndexOfDist = database.insertOverflowOneBatchDist(config.DEVICE_CODES.get(index * clientDevicesNum + m),
									i,
									totalTime,
									errorCount,
									currMaxIndexOfDist,
									random, latencies);
						}
					} catch (SQLException e) {
						LOGGER.error("{} Fail to insert one batch into database becasue {}", Thread.currentThread().getName(), e.getMessage());
					}
				} else {
					System.out.println("unsupported overflow mode:" + config.OVERFLOW_MODE);
					break;
				}
				i++;

				long loopDeltaTime = totalTime.get() - oldTotalTime;

				double loopSecond = loopDeltaTime * 0.000000001d;

				double loopRate = pointsOneLoop / loopSecond;

				if (config.USE_OPS) {
					long delayStart = System.nanoTime();
					if (loopSecond < actualLoopSecond) {
						try {
							Thread.sleep((long) (1000 * (actualLoopSecond - loopSecond)));
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					long delayEnd = System.nanoTime();
					long delayTime = delayEnd - delayStart;
					loopRate = pointsOneLoop / (loopSecond + delayTime * 0.000000001d);
				}

				LOGGER.info("LOOP RATE,{},points/s,LOOP DELTA TIME,{},second", loopRate, loopSecond);
				mySql.saveInsertProcessOfLoop(i, loopRate);
			}
		}


		try {
			database.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.totalTimes.add(totalTime.get());
		this.totalInsertErrorNums.add(errorCount.get());
		this.latenciesOfClients.add(latencies);
		this.downLatch.countDown();
	}

}
