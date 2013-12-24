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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.teknek.collector.CollectorProcessor;
import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.offsetstorage.Offset;
import io.teknek.offsetstorage.OffsetStorage;

/** driver consumes data from a feed partition and inserts it into operators */
public class Driver implements Runnable {
  private FeedPartition fp;
  private DriverNode driverNode;
  private AtomicBoolean goOn;
  private AtomicLong tuplesSeen;
  private OffsetStorage offsetStorage;
  private int offsetCommitInterval;

  /**
   * 
   * @param fp feed partition to consume from
   * @param operator root operator of the driver
   * @param offsetStorage can be null if user does not wish to have offset storage
   */
  public Driver(FeedPartition fp, Operator operator, OffsetStorage offsetStorage, CollectorProcessor collectorProcessor, int offsetCommitInterval ){
    this.fp = fp;
    driverNode = new DriverNode(operator, collectorProcessor);
    this.offsetStorage = offsetStorage;
    goOn = new AtomicBoolean(true);
    tuplesSeen = new AtomicLong(0);
    this.offsetCommitInterval = offsetCommitInterval;
  }
  
  public void initialize(){
    driverNode.initialize();
    fp.initialize();
  }
  
  /**
   * TODO: Currently the way the operators are chained together the offset is incremented as soon as the first operator accepts the tuple.
   * It may be desirable to only increment the offset when the row is completely processed. This turns out to be a non trivial problem because 
   * operators may not emit Tuples making tracking at all leafs impossible.
   * 
   * In any case the "solution" for this could be to allow the driver factory to produce different drivers with different semantics. The semantics
   * vary depending on the semantics of the feed system as well, there is likely no one-size fits all solution.
   * Begin processing the feed in a thread
   */
  public void run(){
    while(goOn.get()){
      ITuple t = new Tuple();
      while (fp.next(t)){
        tuplesSeen.incrementAndGet();
        boolean complete = false;
        int attempts = 0;
        while (attempts++ < driverNode.getCollectorProcessor().getTupleRetry() + 1 && !complete) {
          try {
            driverNode.getOperator().handleTuple(t);
            complete = true;
          } catch (RuntimeException ex){}
        }
        maybeDoOffset();
        t = new Tuple();
      }
    }
  }

  /**
   * To do offset storage we let the topology drain itself out. Then we commit. 
   */
  public void maybeDoOffset(){
    long seen = tuplesSeen.get();
    if (seen % offsetCommitInterval == 0 && offsetStorage != null && fp.supportsOffsetManagement()){
        drainTopology();
        Offset offset = offsetStorage.getCurrentOffset(); 
        offsetStorage.persistOffset(offset);
    }
  }
  
  public DriverNode getDriverNode() {
    return driverNode;
  }

  public void setDriverNode(DriverNode driverNode) {
    this.driverNode = driverNode;
  }
  
  public void drainTopology(){
    DriverNode root = this.driverNode;
    drainTopologyInternal(root);
  }
  
  private void drainTopologyInternal(DriverNode node){
    while (node.getCollectorProcessor().getCollector().size() > 0){
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
      }
    }
    for (DriverNode child: node.getChildren()){
      drainTopologyInternal(child);
    }
  }
  
  public String toString(){
    StringBuilder sb  = new StringBuilder();
    sb.append("Feed Partition " + fp.getPartitionId() + " ");
    sb.append("driver node " + this.driverNode.toString());
    return sb.toString();
  }
  
  public void prettyPrint(){
    System.out.println("+++++++");
    System.out.println("Feed Partition " + fp.getFeed().getClass() + " " );
    System.out.println("Feed Partition " + fp.getPartitionId() + " " );
    System.out.println("-------");
    System.out.println("--"+driverNode.getOperator().getClass().getName());
    for (DriverNode child: driverNode.getChildren() ){
      child.prettyPrint(2);
    }
    System.out.println("+++++++");
  }

  public boolean getGoOn() {
    return goOn.get();
  }

  public void setGoOn(boolean goOn) {
    this.goOn.set(goOn);
  }
  
}