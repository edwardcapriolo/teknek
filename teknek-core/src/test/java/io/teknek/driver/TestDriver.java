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

  public static FeedPartition getPart(){
    Map<String,Object> prop = new HashMap<String,Object>();
    int expectedPartitions = 5;
    int expectedRows = 9;
    prop.put(FixedFeed.NUMBER_OF_PARTITIONS, expectedPartitions);
    prop.put(FixedFeed.NUMBER_OF_ROWS, expectedRows);
    FixedFeed pf = new FixedFeed(prop);
    List<FeedPartition> parts = pf.getFeedPartitions();
    return parts.get(0);
  }
  
  @Test
  public void aTest() throws InterruptedException {
    Driver root = new Driver(getPart(), new Minus1Operator(), null, new CollectorProcessor(), 10);
    root.initialize();
    DriverNode child = new DriverNode(new Times2Operator(), new CollectorProcessor());
    root.getDriverNode().addChild(child);
    
    Thread t = new Thread(root);
    t.start();
    t.join(4000);

    List<Tuple> expected = new ArrayList<Tuple>();
    for (int i = 0; i <= 8; i++) {
      Tuple tup = new Tuple();
      tup.setField("x", (i - 1) * 2);
      expected.add(tup);
    }
    assertExpectedPairs(child, expected);
    
  }

  public static void assertExpectedPairs(DriverNode finalNode, List<Tuple> expected) throws InterruptedException {
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertNotNull(finalNode.getCollectorProcessor().getCollector().peek());
      ITuple got = finalNode.getCollectorProcessor().getCollector().take();
      Assert.assertTrue("element "+i+" comparing expected:" + expected.get(i) + " got:" + got, expected.get(i).equals(got));
    }
    Assert.assertNull("Expected no more elements but found "+finalNode.getCollectorProcessor().getCollector().peek() , finalNode.getCollectorProcessor().getCollector().peek());
  }
  
  @Test
  public void compareTuple(){
    Tuple t = new Tuple();
    t.setField("x", -2);
    Tuple s = new Tuple();
    s.setField("x", -2);
    
    Assert.assertTrue(t.equals(s));
  }
  
 
  

  
}


