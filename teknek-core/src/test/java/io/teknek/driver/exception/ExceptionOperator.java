package io.teknek.driver.exception;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;

/**
 * Throws an exception all the time
 * @author edward
 *
 */
public class ExceptionOperator extends Operator {

  @Override
  public void handleTuple(ITuple t) {
    System.out.println(getClass().getName() + " " + t);
    throw new RuntimeException ("chaos monkey");
  }

}
