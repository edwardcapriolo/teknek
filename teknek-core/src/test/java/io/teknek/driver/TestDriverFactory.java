package io.teknek.driver;

import java.util.ArrayList;
import java.util.List;

import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.plan.TestPlan;

import org.junit.Assert;
import org.junit.Test;

public class TestDriverFactory {

  @Test
  public void aTest() throws InterruptedException{
    Driver driver = DriverFactory.createDriver(TestDriver.getPart(), TestPlan.getPlan());
    Assert.assertEquals(1, driver.getDriverNode().getChildren().size());
    DriverNode minus1Driver = driver.getDriverNode();
    Assert.assertTrue(minus1Driver.getOperator() instanceof Minus1Operator );
    DriverNode times2Driver = driver.getDriverNode().getChildren().get(0);
    Assert.assertTrue(times2Driver.getOperator()+"", times2Driver.getOperator() instanceof Times2Operator);
    Assert.assertEquals(0, times2Driver.getChildren().size() );
    
    driver.initialize();
    Thread t = new Thread(driver);
    t.run();
    t.join(5000);
    //TODO we should attach a collector to this plan and assert results
    /*
    Assert.assertEquals(times2Driver.getCollectorProcessor().getCollector().peek(), 5);
    List<Tuple> expected = new ArrayList<Tuple>();
    for (int i = 0; i < 9; i++) {
      Tuple tup = new Tuple();
      tup.setField("x", (i - 1) * 2);
      expected.add(tup);
    }
    TestDriver.assertExpectedPairs(times2Driver, expected);
    */
  }
}
