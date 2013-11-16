package technique.feed;

import technique.model.ITuple;

public abstract class FeedPartition {

  private Feed feed;
  /**
   * This field uniquely identifies a partition of a feed. In could be critical
   * in cases where you wish client to re-bind to prospective feeds. 
   */
  private String partitionId;
  
  public FeedPartition(Feed feed, String partitionId){
    this.feed = feed;
    this.partitionId = partitionId;
  }
  
  public abstract void initialize();
  
  public abstract boolean next(ITuple t);
  
  public abstract void close();
}