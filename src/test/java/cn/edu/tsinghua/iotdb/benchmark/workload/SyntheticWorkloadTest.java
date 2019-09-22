package cn.edu.tsinghua.iotdb.benchmark.workload;

import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.utils.NameGenerator;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * SyntheticWorkload Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Mar 18, 2019</pre>
 */
public class SyntheticWorkloadTest {

  private static Config config = ConfigParser.INSTANCE.config();
  private NameGenerator nameGenerator = NameGenerator.INSTANCE;

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: getOneBatch()
   */
  @Test
  public void testGetOneBatch() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getPreciseQuery()
   */
  @Test
  public void testGetPreciseQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getRangeQuery()
   */
  @Test
  public void testGetRangeQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getValueRangeQuery()
   */
  @Test
  public void testGetValueRangeQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getAggRangeQuery()
   */
  @Test
  public void testGetAggRangeQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getAggValueQuery()
   */
  @Test
  public void testGetAggValueQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getAggRangeValueQuery()
   */
  @Test
  public void testGetAggRangeValueQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getGroupByQuery()
   */
  @Test
  public void testGetGroupByQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getLatestPointQuery()
   */
  @Test
  public void testGetLatestPointQuery() throws Exception {
//TODO: Test goes here... 
  }


  /**
   * Method: getOrderedBatch()
   */
  @Test
  public void testGetOrderedBatch() throws Exception {

  }

  /**
   * Method: getDistOutOfOrderBatch()
   */
  @Test
  public void testGetDistOutOfOrderBatch() throws Exception {

  }

  /**
   * Method: getLocalOutOfOrderBatch()
   */
  @Test
  public void testGetLocalOutOfOrderBatch() throws Exception {

  }

  /**
   * Method: getGlobalOutOfOrderBatch()
   */
  @Test
  public void testGetGlobalOutOfOrderBatch() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = SyntheticWorkload.getClass().getMethod("getGlobalOutOfOrderBatch");
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
  }

} 
