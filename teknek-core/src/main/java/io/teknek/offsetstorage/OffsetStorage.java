package io.teknek.offsetstorage;

import java.util.Map;

import io.teknek.feed.FeedPartition;
import io.teknek.plan.Plan;

/**
 * OffsetStorage is a system where feeds can track their position in this way in the event
 * of a failure another node can pick up processing where the last node left off
 * @author edward
 *
 */
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
  
  /** get the current offset of the feed
   * @throws UnsupportedOffsetException */
  public abstract Offset getCurrentOffset();

  /**get the last offset to resume from progress */
  public abstract Offset findLatestPersistedOffset();
   
}
