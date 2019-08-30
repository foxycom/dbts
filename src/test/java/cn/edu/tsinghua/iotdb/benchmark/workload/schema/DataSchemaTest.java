package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class DataSchemaTest {
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  @Test
  public void test(){
    testBalanceSplit(100, 30);

  }

  void testBalanceSplit(int deviceNum, int clientNum){
    int preDeviceNum = config.DEVICES_NUMBER;
    int preClientNum = config.CLIENTS_NUMBER;
    config.DEVICES_NUMBER = deviceNum;
    config.CLIENTS_NUMBER = clientNum;
    int mod = config.DEVICES_NUMBER % config.CLIENTS_NUMBER;
    int deviceNumEachClient = config.DEVICES_NUMBER / config.CLIENTS_NUMBER;
    DataSchema dataSchema = DataSchema.getInstance();
    Map<Integer, List<DeviceSchema>> client2Schema = dataSchema.getClientBindSchema();
    for (int clientId : client2Schema.keySet()){
      int deviceNumInClient = client2Schema.get(clientId).size();
      if ( clientId < mod){
        Assert.assertEquals(deviceNumEachClient+1,deviceNumInClient);
      }
      else {
        Assert.assertEquals(deviceNumEachClient,deviceNumInClient);
      }
    }
    config.DEVICES_NUMBER = preDeviceNum;
    config.CLIENTS_NUMBER = preClientNum;
  }


}
