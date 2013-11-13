package technique.feed;

import technique.model.Tuple;

public abstract class FeedPartition {

  private Feed feed;
  
  public FeedPartition(Feed feed){
    this.feed = feed;
  }
  
  public abstract void initialize();
  
  public abstract boolean next(Tuple t);
  
  public abstract void close();
}