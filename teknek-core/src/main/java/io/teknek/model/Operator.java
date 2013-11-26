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
package io.teknek.model;

import java.util.Map;

/**
 * Represents processing logic 
 *
 */
public abstract class Operator {

  protected Map<String,Object> properties;
  protected ICollector collector;
  
  public void setProperties(Map<String,Object> properties){
    this.properties = properties;
  }
  
  public abstract void handleTuple(ITuple t);
  
  public void setCollector(ICollector i){
    this.collector = i;
  }
  
  public ICollector getCollector(){
    return this.collector;
  }
}
