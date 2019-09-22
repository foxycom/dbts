package de.uni_passau.dbts.benchmark.tsdb.griddb;

import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import com.toshiba.mwcloud.gs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Properties;

public class GridDB implements Database {
  private static final Logger LOGGER = LoggerFactory.getLogger(GridDB.class);

  /** GridDB instance. */
  private GridStore store;

  private TimeSeries<Entry> ts;

  /**
   * A schema of an time series entry.
   */
  private class Entry {
    @RowKey private Date time;
    private String bike_id;
    private double s_0;
    private Geometry s_12;
  }

  @Override
  public void init() throws TsdbException {
    Properties props = new Properties();
    props.setProperty("notificationAddress", "127.0.0.1");
    props.setProperty("notificationPort", "20001");
    props.setProperty("clusterName", "test");
    props.setProperty("user", "admin");
    props.setProperty("password", "MyStrongPassword");
    try {
      store = GridStoreFactory.getInstance().getGridStore(props);
    } catch (GSException e) {
      LOGGER.debug("Could not connect to GridDB because: {}", e.getMessage());
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {}

  @Override
  public void close() throws TsdbException {}

  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    try {
      ts = store.putTimeSeries("test", Entry.class);
      ts.createIndex("bike_id");
      ts.createIndex("s_12", IndexType.SPATIAL);
      ts.close();
    } catch (GSException e) {
      LOGGER.debug("Could not register CrateDB schema because: {}", e.getMessage());
      throw new TsdbException(e);
    }
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    long st;
    long en;
    try {
      TimeSeries<Entry> rows = store.getTimeSeries("test", Entry.class);
      Entry entry = new Entry();
      Date d = new Date();
      entry.time = d;
      entry.bike_id = "bike_2";
      st = System.nanoTime();
      rows.put(entry);
      en = System.nanoTime();
    } catch (GSException e) {
      LOGGER.debug("Could not retrieve GridDB ts rows because: {}", e.getMessage());
      return new Status(false, 0, e, e.getMessage());
    }

    return new Status(true, en - st);
  }

  @Override
  public Status precisePoint(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status gpsPathScan(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status identifyTrips(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status offlineBikes(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status lastTimeActivelyDriven(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status downsample(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status lastKnownPosition(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status airPollutionHeatMap(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status distanceDriven(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status bikesInLocation(Query query) {
    return null;
  }
}
