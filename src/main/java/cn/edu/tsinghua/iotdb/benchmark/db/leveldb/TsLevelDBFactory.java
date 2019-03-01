package cn.edu.tsinghua.iotdb.benchmark.db.leveldb;

import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;

public class TsLevelDBFactory implements IDBFactory {

  @Override
  public IDatebase buildDB(long labID) {
    return new TsLevelDB(labID);
  }
}
