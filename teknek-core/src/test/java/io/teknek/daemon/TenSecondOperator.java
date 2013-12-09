package io.teknek.daemon;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;

public class TenSecondOperator extends Operator{

  @Override
  public void handleTuple(ITuple t) {
    try {
      Thread.sleep(10*1000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }

}
