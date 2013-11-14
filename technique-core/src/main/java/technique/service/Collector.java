package technique.service;

import java.util.concurrent.ArrayBlockingQueue;

import technique.model.ICollector;
import technique.model.ITuple;
import technique.model.Tuple;
 
/**
 * Positioned between two operators. emit take and peek work
 * on an underlying blocking queue which should offer flow control.
 *
 */
public class Collector extends ICollector {

  private ArrayBlockingQueue<ITuple> collected;

  public Collector(){
    collected = new ArrayBlockingQueue<ITuple>(4000);
  }
  
  @Override
  public void emit(ITuple out) {
    collected.add(out);
  }

  public ITuple take() throws InterruptedException{
    return collected.take();
  }
  
  public ITuple peek() throws InterruptedException{
    return collected.peek();
  }
  
}
