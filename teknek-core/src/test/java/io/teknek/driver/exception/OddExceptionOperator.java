package io.teknek.driver.exception;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

public class OddExceptionOperator extends Operator {

  @Override
  public void handleTuple(ITuple t) {
    System.out.println("in " + getClass().getName() + " " + t);
    if ((Integer) t.getField("x") % 2 == 1 ){
      throw new RuntimeException("Thats odd " +t);
    }
    Tuple tnew = new Tuple();
    tnew.setField("x", t.getField("x") );
    System.out.println("out " + getClass().getName() + " " + tnew);
    collector.emit(tnew); 
  }

}
