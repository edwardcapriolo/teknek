package io.teknek.model;

public class IdentityOperator extends Operator {
  public void handleTuple(ITuple t) {
    ITuple tnew = new Tuple();
    //tnew.setField("x", ((Integer) t.getField("x")).intValue() - 1);
    for (String field : t.listFields()){
      tnew.setField(field, t.getField(field));
    }
    collector.emit(tnew);
  }
}
