package technique.service;

import java.util.concurrent.ArrayBlockingQueue;

import technique.model.ICollector;
import technique.model.Tuple;
 
/**
 * Positioned between two operators. emit take and peek work
 * on an underlying blocking queue which should offer flow control.
 *
 */
public class Collector extends ICollector {

  private ArrayBlockingQueue<Tuple> collected;

  public Collector(){
    collected = new ArrayBlockingQueue<Tuple>(4000);
  }
  
  @Override
  public void emit(Tuple out) {
    collected.add(out);
  }

  public Tuple take() throws InterruptedException{
    return collected.take();
  }
  
  public Tuple peek() throws InterruptedException{
    return collected.peek();
  }
  
}
