package io.teknek.driver;

import io.teknek.collector.CollectorProcessor;
import io.teknek.driver.Driver;
import io.teknek.driver.DriverNode;
import io.teknek.feed.FeedPartition;
import io.teknek.feed.FixedFeed;
import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;


public class TestDriver {

  private FeedPartition getPart(){
    Map<String,Object> prop = new HashMap<String,Object>();
    int expectedPartitions = 5;
    int expectedRows = 1000;
    prop.put(FixedFeed.NUMBER_OF_PARTITIONS, expectedPartitions);
    prop.put(FixedFeed.NUMBER_OF_ROWS, expectedRows);
    FixedFeed pf = new FixedFeed(prop);
    List<FeedPartition> parts = pf.getFeedPartitions();
    return parts.get(0);
  }
  
  @Test
  public void aTest() throws InterruptedException {
    Driver root = new Driver(getPart(), minus1Operator());
    root.initialize();
    DriverNode child = new DriverNode(times2Operator(), new CollectorProcessor());
    root.getDriverNode().addChild(child);
    
    Thread t = new Thread(root);
    t.run();
    t.join();

    List<Tuple> expected = new ArrayList<Tuple>();
    for (int i = 0; i < 9; i++) {
      Tuple tup = new Tuple();
      tup.setField("x", (i - 1) * 2);
      expected.add(tup);
    }
    assertExpectedPairs(child, expected);
    
  }

  public void assertExpectedPairs(DriverNode finalNode, List<Tuple> expected) throws InterruptedException {
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertNotNull(finalNode.getCollectorProcessor().getCollector().peek());
      ITuple got = finalNode.getCollectorProcessor().getCollector().take();
      Assert.assertTrue("element "+i+" comparing " + expected.get(i) + " " + got, expected.get(i).equals(got));
    }
    Assert.assertNull(finalNode.getCollectorProcessor().getCollector().peek());
  }
  
  @Test
  public void compareTuple(){
    Tuple t = new Tuple();
    t.setField("x", -2);
    Tuple s = new Tuple();
    s.setField("x", -2);
    
    Assert.assertTrue(t.equals(s));
  }
  
  
  public static Operator times2Operator(){
    return new Operator(){
      @Override
      public void handleTuple(ITuple t) {
        ITuple tnew = new Tuple();
        tnew.setField("x", ((Integer) t.getField("x")).intValue() * 2);
        collector.emit(tnew);
      }
    };

  }
  
  public static Operator minus1Operator(){
    return new Operator(){
      @Override
      public void handleTuple(ITuple t) {
        ITuple tnew = new Tuple();
        tnew.setField("x", ((Integer) t.getField("x")).intValue() - 1);
        collector.emit(tnew);
      }
    };
  }
  
  
}
