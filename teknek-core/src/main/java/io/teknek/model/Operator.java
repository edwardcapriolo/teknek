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
}
