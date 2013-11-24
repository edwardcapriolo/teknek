package io.teknek.collector;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import io.teknek.driver.exception.ExEveryOtherTimeOperator;
import io.teknek.model.ICollector;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;

public class TestCollectorProcessor {

  @Test
  public void testRetry(){
    CollectorProcessor cp = new CollectorProcessor();
    cp.setTupleRetry(2);
    final List<ITuple> i = new ArrayList<ITuple>();
    final ICollector collect = new ICollector(){
      @Override
      public void emit(ITuple out) {
        i.add(out);
      }
    };
    ExEveryOtherTimeOperator op = new ExEveryOtherTimeOperator();
    op.setCollector(collect);
    cp.getChildren().add(op);
    cp.handleTupple(new Tuple().withField("x", "y"));
    cp.handleTupple(new Tuple().withField("x", "z"));
    Assert.assertEquals(i.size(), 2);
  }

  @Test
  public void testNoRetry(){
    CollectorProcessor cp = new CollectorProcessor();
    cp.setTupleRetry(0);
    final List<ITuple> i = new ArrayList<ITuple>();
    final ICollector collect = new ICollector(){
      @Override
      public void emit(ITuple out) {
        i.add(out);
      }
    };
    ExEveryOtherTimeOperator op = new ExEveryOtherTimeOperator();
    op.setCollector(collect);
    cp.getChildren().add(op);
    cp.handleTupple(new Tuple().withField("x", "y"));
    cp.handleTupple(new Tuple().withField("x", "z"));
    Assert.assertEquals(i.size(), 1);
    Assert.assertEquals("y", i.get(0).getField("x"));
  }
  
  
}
