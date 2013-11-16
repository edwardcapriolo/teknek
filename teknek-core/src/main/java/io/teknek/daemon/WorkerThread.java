package io.teknek.daemon;

import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.plan.Plan;

import java.util.List;


public class WorkerThread {
  private Plan plan;
  
  public WorkerThread(Plan plan, List<String> otherWorkers){
    this.plan = plan;
  }
  
  /**
   * Here we need to determine 
   */
  public void init(){
    Feed feed = null;
    try {
      feed = (Feed) Class.forName(plan.getFeedDesc().getFeedClass()).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      
    }
    List<FeedPartition> feedPartitions = feed.getFeedPartitions();
    for (int i = 0; i < feedPartitions.size(); i++) {
      FeedPartition aPartition = feedPartitions.get(i);
      
    }
  }
}
