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
package io.teknek.plan;

import java.util.Map;

public class FeedDesc extends DynamicInstantiatable {
  

  @SuppressWarnings("rawtypes")
  private Map properties;
  private String name;
  
  public FeedDesc(){
    
  }

  public Map getProperties() {
    return properties;
  }

  public void setProperties(Map properties) {
    this.properties = properties;
  }
  
  public FeedDesc withFeedClass(String feedClass) {
    this.theClass = feedClass;
    return this;
  }
  
  public FeedDesc withProperties(Map properties) {
    this.properties = properties;
    return this;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
  
  public FeedDesc withName(String name){
    setName(name);
    return this;
  }
}
