package technique.feed;

import technique.model.ITuple;

public abstract class FeedPartition {

  private Feed feed;
  
  public FeedPartition(Feed feed){
    this.feed = feed;
  }
  
  public abstract void initialize();
  
  public abstract boolean next(ITuple t);
  
  public abstract void close();
}