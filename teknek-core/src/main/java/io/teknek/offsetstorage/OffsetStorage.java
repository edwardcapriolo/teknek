package io.teknek.offsetstorage;

import java.util.Map;

import io.teknek.feed.FeedPartition;
import io.teknek.plan.Plan;

public abstract class OffsetStorage {
  protected FeedPartition feedPartiton;
  protected Plan plan;
  protected Map<String,String> properties;
  
  public OffsetStorage(FeedPartition feedPartition, Plan plan, Map<String,String> properties){
    this.feedPartiton = feedPartition;
    this.plan = plan;
    this.properties = properties;
  }

  /** write offset to whatever the underlying storage is **/
  public abstract void persistOffset(Offset o);
  
  /** get the current offset of the feed*/
  public abstract Offset getCurrentOffset();

  /**get the last offset to resume from progress */
  public abstract Offset findLatestPersistedOffset();
   
}
