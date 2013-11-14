package technique.feed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import technique.feed.Feed;
import technique.feed.FeedPartition;
import technique.model.ITuple;
import technique.model.Tuple;

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
    List<FeedPartition> res= new ArrayList<FeedPartition>();
    for (int i=0;i<numberOfPartitions;i++){
      FixedFeedPartition sf = new FixedFeedPartition(this);
      sf.setPartitionId(i);
      res.add(sf);
    }
    return res;
  }
  
  public Map<String, String> getSuggestedBindParams() {
    return new HashMap<String, String>();
  }
}

class FixedFeedPartition extends FeedPartition {

  private int partitionId;
  private int current = 0;
  private int max = 10;
  
  public FixedFeedPartition(Feed f) {
    super(f);
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {                                                                                                                                                              
    this.partitionId = partitionId;
  }

  @Override
  public boolean next(ITuple t) {
    t.setField("x", new Integer(current++));
    return current < max;
  }

  @Override
  public void initialize() {
    
  }

  @Override
  public void close() {

  }
    
}

