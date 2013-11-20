package io.teknek.offsetstorage;

import java.util.GregorianCalendar;

import io.teknek.feed.FeedPartition;
import io.teknek.plan.Plan;

public abstract class OffsetStorage {
  protected FeedPartition part;
  protected Plan plan;
  
  public OffsetStorage(FeedPartition feedPartition, Plan plan){
    this.part = feedPartition;
    this.plan = plan;
  }

  /** write offset to whatever the underlying storage is **/
  public abstract void persistOffset(Offset o);
  
  /** get the current offset of the feed*/
  public abstract Offset getCurrentOffset();

  /**get the last offset to resume from progress */
  public abstract Offset findLatestPersistedOffset();
   
}
