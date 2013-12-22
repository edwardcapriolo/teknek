/*
 Copyright 2013 Edward Capriolo, Matt Landolf, Lodwin Cueto

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.teknek.driver;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;

import io.teknek.collector.Collector;
import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;

import io.teknek.feed.TestFixedFeed;
import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.TestPlan;

import org.junit.Assert;
import org.junit.Test;

public class TestDriverFactory {

  @Test
  public void aTest() throws InterruptedException{
    Driver driver = DriverFactory.createDriver(TestDriver.getPart(), TestPlan.getPlan());
    Assert.assertEquals(1, driver.getDriverNode().getChildren().size());
    DriverNode minus1Driver = driver.getDriverNode();
    Assert.assertTrue(minus1Driver.getOperator() instanceof Minus1Operator );
    DriverNode times2Driver = driver.getDriverNode().getChildren().get(0);
    Assert.assertTrue(times2Driver.getOperator()+"", times2Driver.getOperator() instanceof Times2Operator);
    Assert.assertEquals(0, times2Driver.getChildren().size() );
    
    driver.initialize();
    Thread t = new Thread(driver);
    t.start();
    t.join(5000);
    //TODO we should attach a collector to this plan and assert results
    /*
    Assert.assertEquals(times2Driver.getCollectorProcessor().getCollector().peek(), 5);
    List<Tuple> expected = new ArrayList<Tuple>();
    for (int i = 0; i < 9; i++) {
      Tuple tup = new Tuple();
      tup.setField("x", (i - 1) * 2);
      expected.add(tup);
    }
    TestDriver.assertExpectedPairs(times2Driver, expected);
    */
  }
   
  public static OperatorDesc buildGroovyOperatorDesc(){
    OperatorDesc o = new OperatorDesc();
    o.setSpec("groovy");
    o.setTheClass("ATry");
    o.setScript("import io.teknek.driver.Minus1Operator\n"+"public class ATry extends Minus1Operator { \n }");
    return o;
  }
  
  public static FeedDesc buildGroovyFeedDesc(){
    FeedDesc o = new FeedDesc();
    o.setSpec("groovy");
    o.setTheClass("FTry");
    o.setProperties(TestFixedFeed.buildFeedProps());
    o.setScript("import io.teknek.feed.FixedFeed\n" + "public class FTry extends FixedFeed { "
            + "public FTry(Map<String,Object> properties){ \n" + "super(properties);\n"
            + "numberOfPartitions = (Integer) super.properties.get(NUMBER_OF_PARTITIONS);\n"
            + "numberOfRows = (Integer) super.properties.get(NUMBER_OF_ROWS);\n" + "} \n }");
    return o;
  }
  
  public static FeedDesc buildPureGroovyFeedDesc(){
    FeedDesc o = new FeedDesc();
    o.setSpec("groovy");
    o.setTheClass("GTry");
    o.setName("GTry");
    o.setProperties(TestFixedFeed.buildFeedProps());
    o.setScript("import io.teknek.feed.*\n"
            + "import io.teknek.model.*\n" 
            + "public class GTry extends Feed { \n"
            + "public static final String NUMBER_OF_PARTITIONS = \"number.of.partitions\"; \n"
            + "public static final String NUMBER_OF_ROWS = \"number.of.rows\"; \n"
            + "protected int numberOfPartitions; \n"
            + "protected int numberOfRows; \n"
            + "public GTry(Map<String,Object> properties){ \n" 
            + "  super(properties);\n"
            + "}\n"
            +" public List<FeedPartition> getFeedPartitions() { \n"
            +"   numberOfPartitions = ((Number) super.properties.get(NUMBER_OF_PARTITIONS)).intValue(); \n"
            +"   numberOfRows = ((Number) super.properties.get(NUMBER_OF_ROWS)).intValue(); \n"
            +"   List<FeedPartition> res = new ArrayList<FeedPartition>();\n"
            +"   for (int i = 0; i < numberOfPartitions; i++) \n"
            +"     res.add(new GTryPartition(this, String.valueOf(i))); \n"
            +"   return res; \n"
            +"  } \n"
            +"  public Map<String, String> getSuggestedBindParams() { \n"
            +"    return new HashMap<String, String>(); \n"
            +"  } \n"
            +"} \n"
            +"class GTryPartition extends FeedPartitionAdapter { \n"
            +"  private int current = 0; \n"
            +"  private int max = 10; \n"
            +"  public GTryPartition(Feed f, String partitionId) { \n"
            +"    super(f , partitionId); \n"
            +"    if (f.getProperties().get(GTry.NUMBER_OF_ROWS)!=null){ \n"
            +"      max = Integer.parseInt( f.getProperties().get(GTry.NUMBER_OF_ROWS).toString() ); \n"
            +"    } \n"
            +"  } \n"
            +"  @Override \n"
            +"  public boolean next(ITuple t) { \n"
            +"    t.setField(\"x\", new Integer(current)); \n"
            +"    return current++ < max; \n"
            +"  } \n"
            +"} \n");
   
    return o;
  }
  
  @Test
  public void testPureGroovyFeed(){
    Feed f = DriverFactory.buildFeed(buildPureGroovyFeedDesc());
    Assert.assertEquals("GTry", f.getClass().getSimpleName());
    List<FeedPartition> parts = f.getFeedPartitions();
    ITuple t = new Tuple();
    parts.get(0).next(t);
    Assert.assertEquals(parts.get(0).getClass().getSimpleName(), "GTryPartition");
    Assert.assertEquals(0, t.getField("x"));
  }
  
  @Test
  public void feedTest(){
    Feed f = DriverFactory.buildFeed(buildGroovyFeedDesc());
    Assert.assertNotNull(f);
  }
  
  @Test
  public void operatorTest(){
    Operator operator = DriverFactory.buildOperator(buildGroovyOperatorDesc());
    Assert.assertNotNull(operator);
    Assert.assertEquals ("ATry", operator.getClass().getName());
  }
  
  public static OperatorDesc getIdentityGroovyOperator(){
    OperatorDesc o = new OperatorDesc();
    o.setSpec("groovyclosure");
    o.setTheClass("groovy_identity");
    o.setName("groovy_identity");
    o.setScript("{ tuple, collector ->  collector.emit(tuple) ; println(tuple) }");
    return o;
  }
  
  @Test
  public void groovyClosureTest() throws InterruptedException{
    OperatorDesc o = getIdentityGroovyOperator();
    Operator operator = DriverFactory.buildOperator(o);
    operator.setCollector(new Collector());
    Assert.assertNotNull(operator);
    Assert.assertEquals ("io.teknek.model.GroovyOperator", operator.getClass().getName());
    ITuple t = new Tuple();
    t.setField("x", 5);
    operator.handleTuple(t);
    Assert.assertEquals(5, ((Collector) operator.getCollector()).take().getField("x"));
  }
  
  @Test
  public void testUrlLoader() throws MalformedURLException {
    String cname = "io.teknek.model.CopyOfIdentityOperator";
    OperatorDesc o = new OperatorDesc();
    o.setTheClass(cname);
    o.setSpec("url");
    File f = new File("src/test/resources/id.jar");
    Assert.assertTrue(f.exists());
    o.setScript(f.toURL().toString());
    Operator oo = DriverFactory.buildOperator(o);
    Assert.assertEquals(cname, oo.getClass().getName());
  }
}
