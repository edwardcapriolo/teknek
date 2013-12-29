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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  private long tuplesSeen;
  private OffsetStorage offsetStorage;
  private int offsetCommitInterval;
  private ExecutorService feedExecutor;
  
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
    tuplesSeen = 0;
    this.offsetCommitInterval = offsetCommitInterval;
  }
  
  public void initialize(){
    driverNode.initialize();
    fp.initialize();
    feedExecutor = Executors.newSingleThreadExecutor();
  }
  
  public void run(){
    boolean getInFuture = false;
    boolean hasNext = false;
    do {
      if (!this.getGoOn()){
        break;
      }
      final ITuple t = new Tuple();
      if (getInFuture) {
        /*
         * This code is in place because if driver is blocking on next() the driver will not be aware
         * it has been asked to shut down. Maybe this could would not be needed to something should
         * be interupted.
         */
        Callable<Boolean> c = new Callable<Boolean>(){
          @Override
          public Boolean call() throws Exception {
            return fp.next(t);
          }
        };
        Future<Boolean> f = feedExecutor.submit(c);
        try {
          //TODO this timeout could be problematic in slow feeds...
          //maybe better for give this a long wait and check the goOnStatus periodically
          //using future.isDone(), but that is a spinlock
          hasNext = f.get(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e1) {
          //f.cancel(mayInterruptIfRunning) ??
          continue;
        }
      } else {
        hasNext = fp.next(t);
      }
      tuplesSeen++;
      boolean complete = false;
      int attempts = 0;
      while (attempts++ < driverNode.getCollectorProcessor().getTupleRetry() + 1 && !complete) {
        try {
          driverNode.getOperator().handleTuple(t);
          complete = true;
        } catch (RuntimeException ex) {
        }
      }
      maybeDoOffset();
      if (!hasNext) {
        break;
      }
    } while (goOn.get());
    feedExecutor.shutdown();
  }
  
  

  /**
   * To do offset storage we let the topology drain itself out. Then we commit. 
   */
  public void maybeDoOffset(){
    long seen = tuplesSeen;
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