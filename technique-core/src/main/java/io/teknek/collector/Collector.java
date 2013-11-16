package io.teknek.collector;

import io.teknek.model.ICollector;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;

import java.util.concurrent.ArrayBlockingQueue;

 
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
