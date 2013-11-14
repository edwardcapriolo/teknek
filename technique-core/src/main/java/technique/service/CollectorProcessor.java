package technique.service;

import java.util.ArrayList;
import java.util.List;

import technique.model.ITuple;
import technique.model.Operator;
import technique.model.Tuple;

public class CollectorProcessor implements Runnable {
  Collector collector;
  List<Operator> children;
  boolean goOn = true;
  
  public CollectorProcessor(){
    children = new ArrayList<Operator>();
    collector = new Collector();
  }
  
  public void run(){
    while(goOn){
      try {
        ITuple tuple = collector.take();
        for (Operator o: children){
          o.handleTuple(tuple);
        }
      } catch (InterruptedException e) {       
        e.printStackTrace();
      }
    }
  }

  public Collector getCollector() {
    return collector;
  }

  public List<Operator> getChildren() {
    return children;
  }
  
  
  
}
