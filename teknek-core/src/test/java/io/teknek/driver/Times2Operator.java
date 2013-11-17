package io.teknek.driver;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

public class Times2Operator extends Operator {
  public void handleTuple(ITuple t) {
    ITuple tnew = new Tuple();
    tnew.setField("x", ((Integer) t.getField("x")).intValue() * 2);
    collector.emit(tnew);
  }
}
