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
