package cn.edu.tsinghua.iotdb.benchmark.db.leveldb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.leveldb.LevelTimeSeriesDBFactory;
import edu.tsinghua.k1.rocksdb.RocksDBTimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import org.iq80.leveldb.Options;

public class LevelDB {

  public ITimeSeriesDB getTimeSeriesDB() {
    return timeSeriesDB;
  }

  private ITimeSeriesDB timeSeriesDB = null;
  private static Config config  = ConfigDescriptor.getInstance().getConfig();

  private static class LevelDBHolder{
    private static final LevelDB INSTANCE = new LevelDB();
  }

  public static LevelDB getInstance(){
    return LevelDBHolder.INSTANCE;
  }

  LevelDB(){
    File file = new File(config.GEN_DATA_FILE_PATH);
    System.out.println("creating timeSeriesDB...");

    try {
      timeSeriesDB = makeTimeSeriesDB(file);

    } catch (IOException | SQLException e) {
      e.printStackTrace();
    } finally {
      // if the is not null, close it
    }
  }

  private ITimeSeriesDB makeTimeSeriesDB(File file) throws IOException, SQLException {
    switch (config.STORE_MODE) {
      case 1:
        Options optionsLevel;
        optionsLevel = new Options();
        optionsLevel.createIfMissing(true);
        optionsLevel.writeBufferSize(16<<20);
        optionsLevel.cacheSize(4L<<30);
        optionsLevel.maxOpenFiles(1<<20);
        return LevelTimeSeriesDBFactory.getInstance().openOrCreate(file, optionsLevel);
      case 2:
        org.rocksdb.Options optionsRocks;
        optionsRocks = new org.rocksdb.Options();
        optionsRocks.setCreateIfMissing(true);
        optionsRocks.setWriteBufferSize(10<<20);
        return RocksDBTimeSeriesDBFactory.getInstance().openOrCreate(file, optionsRocks);
      default:
        throw new SQLException("unsupported STORE_MODE: " + config.STORE_MODE);
    }
  }

}
