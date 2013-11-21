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
package io.teknek.feed;

import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;
import io.teknek.plan.FeedDesc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;


public class TestFixedFeed {

  private static final int EXPECTED_PARTITIONS = 5;
  private static final int EXPECTED_ROWS = 1000;
  
  public static Map<String,Object> buildFeedProps(){
    Map<String,Object> props = new HashMap<String,Object>();
    props.put(FixedFeed.NUMBER_OF_PARTITIONS, EXPECTED_PARTITIONS);
    props.put(FixedFeed.NUMBER_OF_ROWS, EXPECTED_ROWS);
    return props;
  }
  
  @Test
  public void testFeed(){
    FixedFeed pf = new FixedFeed(buildFeedProps());
    List<FeedPartition> parts = pf.getFeedPartitions();
    Assert.assertEquals(EXPECTED_PARTITIONS, parts.size());
    ITuple t = new Tuple();
    
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
  
  @Test
  public void testSetOffset(){
    FixedFeed pf = new FixedFeed(buildFeedProps());
    List<FeedPartition> parts = pf.getFeedPartitions();
    Assert.assertEquals(EXPECTED_PARTITIONS, parts.size());
    ITuple t = new Tuple();
    
    parts.get(0).setOffset("5");
    parts.get(0).next(t);
    Assert.assertEquals( t.getField("x"), 5);

  }
  
  
  @Test
  public void testReflection(){
    FeedDesc fd = new FeedDesc();
    fd.setFeedClass(FixedFeed.class.getCanonicalName());
    fd.setProperties(buildFeedProps());
    Feed feed = Feed.buildFeed(fd);
    Assert.assertEquals(TestFixedFeed.EXPECTED_PARTITIONS, feed.getFeedPartitions().size());
  }
}
