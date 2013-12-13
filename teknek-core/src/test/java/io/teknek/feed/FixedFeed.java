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

import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class FixedFeed extends Feed {
  public static final String NUMBER_OF_PARTITIONS = "number.of.partitions";
  public static final String NUMBER_OF_ROWS = "number.of.rows";
  int numberOfPartitions;
  int numberOfRows;
  
  public FixedFeed(Map<String,Object> properties){
    super(properties);
    numberOfPartitions = (Integer) super.properties.get(NUMBER_OF_PARTITIONS);
    numberOfRows = (Integer) super.properties.get(NUMBER_OF_ROWS);
  }

  

  public List<FeedPartition> getFeedPartitions() {
    List<FeedPartition> res = new ArrayList<FeedPartition>();
    for (int i = 0; i < numberOfPartitions; i++) {
      FixedFeedPartition sf = new FixedFeedPartition(this, String.valueOf(i));
      res.add(sf);
    }
    return res;
  }
  
  public Map<String, String> getSuggestedBindParams() {
    return new HashMap<String, String>();
  }
}

class FixedFeedPartition extends FeedPartition {

  private int current = 0;
  private int max = 10;
  
  public FixedFeedPartition(Feed f, String partitionId) {
    super(f , partitionId);
    if (f.getProperties().get(FixedFeed.NUMBER_OF_ROWS)!=null){
      max = Integer.parseInt( f.getProperties().get(FixedFeed.NUMBER_OF_ROWS).toString() );
    }
  }

  @Override
  public boolean next(ITuple t) {
    t.setField("x", new Integer(current));
    return current++ < max;
  }

  @Override
  public void initialize() {
    
  }

  @Override
  public void close() {

  }
    
  @Override
  public String getOffset() {
    return current+"";
  }

  @Override
  public boolean supportsOffsetManagement() {
    return true;
  }

  @Override
  public void setOffset(String offset) {
    this.current = Integer.parseInt(offset);
  }
}

