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

import java.util.List;
import java.util.Map;

/**
 * An abstraction over a data source. A feed must have one or more partitions. Partitions allow the
 * feed to be processed in parallel.
 * 
 * Based on the properties supplied at the construction of the feed, it is imperative that the feed
 * repeatedly generate the same number of partitions with the same identifiers. This is required
 * because the framework needs to be able to coordinate processing the partitions of the feed.
 * 
 */
public abstract class Feed {

  /**
   * A per instance identifier of a feed
   */
  private String name;
  /**
   * The properties of the feed
   */
  protected Map<String,Object> properties;

  /**
   * Note: Do not initialize variables in the constructor. The properties are mutable up until
   * getFeedPartitions() is called
   * 
   * @param properties
   */
  public Feed(Map<String,Object> properties){
    this.properties = properties;
  }
  
  /**
   * Method is called by the framework, based on the class and optionally the properties it produces
   * one or more partitions. The list of partitions returned must be deterministic.
   * 
   * @return a list of partitions for the feed based on properties
   */
  public abstract List<FeedPartition> getFeedPartitions();
  /**
   * Map of configuration options to explanation. Inline documentation
   * and auto-discovery when building plans
   * @return
   */
  public abstract Map<String,String> getSuggestedBindParams();
  
  
  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }
  
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Feed withName(String name) {
    this.setName(name);
    return this;
  }
}

