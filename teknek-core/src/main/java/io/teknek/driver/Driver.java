package io.teknek.driver;

import java.util.List;

import io.teknek.collector.CollectorProcessor;
import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;


/** driver consumes data from a feed partition and inserts it into operators */
public class Driver implements Runnable {
  private FeedPartition fp;
  private DriverNode driverNode;

  public Driver(FeedPartition fp, Operator operator){
    this.fp = fp;
    CollectorProcessor cp = new CollectorProcessor();
    driverNode = new DriverNode(operator, cp);
  }
  
  public void initialize(){
    driverNode.initialize();
  }
  
  /**
   * Begin processing the feed in a thread
   */
  public void run(){
    ITuple t = new Tuple();
    while (fp.next(t)){
      driverNode.getOperator().handleTuple(t);
      t = new Tuple();
    }
  }

  public DriverNode getDriverNode() {
    return driverNode;
  }

  public void setDriverNode(DriverNode driverNode) {
    this.driverNode = driverNode;
  }
  
  public String toString(){
    StringBuilder sb  = new StringBuilder();
    sb.append("Feed Partition "+fp.getPartitionId()+" " );
    sb.append("driver node "+ this.driverNode.toString());
    return sb.toString();
  }
  
}