package io.teknek.model;

import io.teknek.collector.Collector;
import groovy.lang.Closure;

public class GroovyOperator extends Operator{

  private Closure closure;
  
  public GroovyOperator(Closure closure){
    this.closure = closure;
  }
  
  @Override
  public void handleTuple(ITuple t) {
    closure.call(t, getCollector());
  }
  

}
