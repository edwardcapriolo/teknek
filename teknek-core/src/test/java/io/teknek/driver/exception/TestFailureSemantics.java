package io.teknek.driver.exception;

import org.junit.Assert;
import org.junit.Test;

import io.teknek.driver.Driver;
import io.teknek.driver.DriverFactory;
import io.teknek.driver.DriverNode;
import io.teknek.driver.Minus1Operator;
import io.teknek.driver.TestDriver;
import io.teknek.driver.Times2Operator;
import io.teknek.feed.FixedFeed;
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
        .withProperties( MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,10)))
     .withRootOperator(new OperatorDesc(new OddExceptionOperator()));
    plan.getRootOperator().withNextOperator(new OperatorDesc(new ExEveryOtherTimeOperator())
      .withNextOperator( new OperatorDesc(new Minus1Operator())));
    plan.getRootOperator().withNextOperator(new OperatorDesc(new ExceptionOperator()));
    plan.getRootOperator().withNextOperator(new OperatorDesc(new Times2Operator()));
    return plan;
  }
  
  @Test
  public void goodLuck() throws InterruptedException{
    Plan plan = createAPlanForDisaster();
    Driver driver = DriverFactory.createDriver(TestDriver.getPart(), plan);
    driver.initialize();
    driver.prettyPrint();
    DriverNode minus1Node = driver.getDriverNode().getChildren().get(0)
            .getChildren().get(0);
    Assert.assertTrue( minus1Node.getOperator() instanceof Minus1Operator );
    
    Thread t = new Thread(driver);
    t.start();
    t.sleep(5000);
    
    System.out.println("output " + minus1Node.getCollectorProcessor().getCollector().peek());

  }
}
