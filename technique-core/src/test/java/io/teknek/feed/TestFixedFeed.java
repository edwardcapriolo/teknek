package io.teknek.feed;

import io.teknek.feed.FeedPartition;
import io.teknek.model.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;


public class TestFixedFeed {

  @Test
  public void testFeed(){
    Map<String,Object> prop = new HashMap<String,Object>();
    int expectedPartitions = 5;
    int expectedRows = 1000;
    prop.put("number.of.partitions", expectedPartitions);
    prop.put("number.of.rows", expectedRows);
    FixedFeed pf = new FixedFeed(prop);
    List<FeedPartition> parts = pf.getFeedPartitions();
    Assert.assertEquals(expectedPartitions, parts.size());
    Tuple t = new io.teknek.model.Tuple();
    
    parts.get(0).next(t);
    Assert.assertEquals( t.getField("x"), 0);
    parts.get(0).next(t);
    Assert.assertEquals(t.getField("x"), 1);
    parts.get(0).next(t);
    Assert.assertEquals(t.getField("x"), 2);

    parts.get(1).next(t);
    Assert.assertEquals(t.getField("x"), 0);
    parts.get(1).next(t);
    Assert.assertEquals(t.getField("x"), 1);
  }
}
