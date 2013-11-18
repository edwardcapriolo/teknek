/*
Copyright 2013 Edward Capriolo, Matt Landolf, Lodwin Cueto

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.teknek.feed;

import io.teknek.plan.FeedDesc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * represents some data source. A feed breaks down to one or more partition
 *
 */
public abstract class Feed {

  private String name;
  protected Map<String,Object> properties;
  
  public Feed(Map<String,Object> properties){
    this.properties = properties;
  }
  
  /**
   * Method is called by the framework, based on the class and optionally the properties it produces one or more 
   * partitions. The list of partitions returned must be deterministic
   * @return a list of partitions for the feed based on properties
   */
  public abstract List<FeedPartition> getFeedPartitions();
  /**
   * Map of configuration options to explanation. Inline documentation
   * and auto-discovery when building plans
   * @return
   */
  public abstract Map<String,String> getSuggestedBindParams();
  
  
  public Map<String, Object> getProperties() {
    return properties;
  }

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

