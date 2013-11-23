package io.teknek.driver.exception;

import java.util.concurrent.atomic.AtomicLong;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

/**
 * handleTuple throws an exception every other invocation
 * @author edward
 *
 */
public class ExEveryOtherTimeOperator extends Operator {

  private AtomicLong counter = new AtomicLong(0);
  
  @Override
  public void handleTuple(ITuple t) {
    System.out.println("in "+ getClass().getName() + " " + t);
    if (counter.getAndIncrement() % 2 == 1){
      throw new RuntimeException("I am always performing at half capacity" +t);
    } else {
      Tuple tnew = new Tuple();
      tnew.setField("x", t.getField("x"));
      System.out.println("out " + getClass().getName() + " " + tnew);
      collector.emit(tnew); 
    }
  }
  
}
