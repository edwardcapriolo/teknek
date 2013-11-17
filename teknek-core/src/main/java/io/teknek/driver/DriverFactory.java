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

import io.teknek.collector.CollectorProcessor;
import io.teknek.feed.FeedPartition;
import io.teknek.model.Operator;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;

import java.util.List;

public class DriverFactory {

  public static Driver createDriver(FeedPartition feedPartition, Plan plan){
    OperatorDesc desc = plan.getRootOperator();
    Operator oper = null;
    try {
      oper = (Operator) Class.forName(desc.getOperatorClass()).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    Driver driver = new Driver(feedPartition, oper);
    DriverNode root = driver.getDriverNode();
    
    recurseOperatorAndDriverNode(desc, root);
    return driver;
  }
  
  private static void recurseOperatorAndDriverNode(OperatorDesc desc, DriverNode node){
    List<OperatorDesc> children = desc.getChildren();
    for (OperatorDesc childDesc: children){
      Operator oper = null;
      try {
        oper = (Operator) Class.forName(childDesc.getOperatorClass()).newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      DriverNode childNode = new DriverNode(oper, new CollectorProcessor());
      node.addChild(childNode);
      recurseOperatorAndDriverNode(childDesc, childNode);
    }
  }
}
