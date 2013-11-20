/*
Copyright 2013 Edward Capriolo, Matt Landolf, Lodwin Cueto

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
    t.start();
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
