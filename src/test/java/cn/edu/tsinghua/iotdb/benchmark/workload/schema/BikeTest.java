package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/** 
* Bike Tester.
* 
* @author <Authors name> 
* @since <pre>Mar 18, 2019</pre> 
* @version 1.0 
*/ 
public class BikeTest {
/** 
* 
* Method: CalGroupId()
* 
*/ 
@Test
public void testCalGroupId() throws Exception {
  int g1 = Bike.calGroupId(4, 10, 3);
  assertEquals(1,g1 );
  int g2 = Bike.calGroupId(3, 10, 3);
  assertEquals(0, g2);

  int g3 = Bike.calGroupId(30, 100, 30);
  assertEquals(7, g3);
  int g4 = Bike.calGroupId(40, 100, 30);
  assertEquals(10, g4);
  int g5 = Bike.calGroupId(0, 1, 3);
  assertEquals(0, g5);
} 

} 
