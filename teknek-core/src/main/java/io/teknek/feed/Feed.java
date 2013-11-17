package io.teknek.feed;

import io.teknek.plan.FeedDesc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
  
  //TODO this method might go better in driver or some other place that does not tangle the classes
  //TODO we need something better then runtime exception here
  /**
   * Build a feed using reflection
   * @param feedDesc
   * @return
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static Feed buildFeed(FeedDesc feedDesc){
    Feed feed = null;
    Class [] paramTypes = new Class [] { Map.class };    
    Constructor<Feed> feedCons = null;
    try {
      feedCons = (Constructor<Feed>) Class.forName(feedDesc.getFeedClass()).getConstructor(
              paramTypes);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try {
      feed = feedCons.newInstance(feedDesc.getProperties());
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    return feed;
  }
}

