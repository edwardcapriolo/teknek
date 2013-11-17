package io.teknek.daemon;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;

public class BeLoudOperator extends Operator {

  @Override
  public void handleTuple(ITuple t) {
    System.out.println(t);
  }
}