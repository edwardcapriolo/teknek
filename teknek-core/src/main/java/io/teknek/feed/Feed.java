package io.teknek.feed;

import java.util.List;
import java.util.Map;

/**
 * represents some data source
 *
 */
public abstract class Feed {

  private String name;
  protected Map<String,Object> properties;
  
  public Feed(Map<String,Object> properties){
    this.properties = properties;
  }
  
  public abstract List<FeedPartition> getFeedPartitions();
  /**
   * Map of configuration options to explanation. Inline documentation
   * and auto-discovery when building plans
   * @return
   */
  public abstract Map<String,String> getSuggestedBindParams();
}

