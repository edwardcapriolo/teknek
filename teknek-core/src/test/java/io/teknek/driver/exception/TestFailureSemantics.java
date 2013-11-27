package io.teknek.driver.exception;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import io.teknek.driver.Driver;
import io.teknek.driver.DriverFactory;
import io.teknek.driver.DriverNode;
import io.teknek.driver.Minus1Operator;
import io.teknek.driver.TestDriver;
import io.teknek.driver.Times2Operator;
import io.teknek.feed.FixedFeed;
import io.teknek.model.Tuple;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.plan.TestPlan;
import io.teknek.util.MapBuilder;

public class TestFailureSemantics {

  /**
   * OddExceptionOperator -- ExEveryOtherTimeOperator -- Minus1Operator
   *                      -- ExceptionOperator
   *                      -- Times2Operator
   * @return
   */
  public static Plan createAPlanForDisaster(){
    Plan plan = new Plan()
    .withFeedDesc(new FeedDesc()
      .withFeedClass(FixedFeed.class.getName())
        .withProperties(MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,10)))
     .withRootOperator(new OperatorDesc(new OddExceptionOperator()));
    plan.getRootOperator().withNextOperator(new OperatorDesc(new ExEveryOtherTimeOperator())
      .withNextOperator(new OperatorDesc(new Minus1Operator())));
    plan.getRootOperator().withNextOperator(new OperatorDesc(new ExceptionOperator()));
    plan.getRootOperator().withNextOperator(new OperatorDesc(new Times2Operator()));
    return plan;
  }
  
  @Ignore
  @Test
  public void goodLuck() throws InterruptedException{
    Plan plan = createAPlanForDisaster();
    Driver driver = DriverFactory.createDriver(TestDriver.getPart(), plan);
    driver.initialize();
    driver.prettyPrint();
    DriverNode minus1Node = driver.getDriverNode().getChildren().get(0).getChildren().get(0);
    minus1Node.getCollectorProcessor().setGoOn(false);
    DriverNode times2Driver = driver.getDriverNode().getChildren().get(2);
    times2Driver.getCollectorProcessor().setGoOn(false);
    DriverNode exceptionDriver = driver.getDriverNode().getChildren().get(1);
    exceptionDriver.getCollectorProcessor().setGoOn(false);

    Thread t = new Thread(driver);
    t.start();
    Thread.sleep(6000);
    
    assertMinus1Node(minus1Node);
    assertTimes2Driver(times2Driver);
    Assert.assertNull(exceptionDriver.getCollectorProcessor().getCollector().peek());
    
  }
  
  public void assertTimes2Driver(DriverNode times2driver) throws InterruptedException {
    List<Tuple> expected = new ArrayList<Tuple>();
    for (int i = 2  ; i < 9; i=i+2) {
      Tuple tup = new Tuple();
      tup.setField("x", i*2);
      expected.add(tup);
    }
    TestDriver.assertExpectedPairs(times2driver, expected);
  }
  
  public void assertMinus1Node(DriverNode minus1Node) throws InterruptedException{
    List<Tuple> expected = new ArrayList<Tuple>();
    for (int i = 1; i < 9; i = i + 2) {
      Tuple tup = new Tuple();
      tup.setField("x", i);
      expected.add(tup);
    }
    TestDriver.assertExpectedPairs(minus1Node, expected);
  }
}
