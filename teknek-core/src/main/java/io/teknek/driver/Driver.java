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

  /**
   * 
   * @param fp feed partition to consume from
   * @param operator root operator of the driver
   * @param offsetStorage can be null if user does not wish to have offset storage
   */
  public Driver(FeedPartition fp, Operator operator, OffsetStorage offsetStorage){
    this.fp = fp;
    CollectorProcessor cp = new CollectorProcessor();
    driverNode = new DriverNode(operator, cp);
    this.offsetStorage = offsetStorage;
    goOn = new AtomicBoolean(true);
    tuplesSeen = new AtomicLong(0);
  }
  
  public void initialize(){
    driverNode.initialize();
  }
  
  /**
   * Begin processing the feed in a thread
   */
  public void run(){
    while(goOn.get()){
      ITuple t = new Tuple();
      while (fp.next(t)){
        driverNode.getOperator().handleTuple(t);
        maybeDoOffset();
        t = new Tuple();
      }
    }
  }

  /**
   * We mark the offset every N rows. It would probably be better
   * to mark in a background thread based on time or make it plugable, but this
   * gets the point across for now
   */
  public void maybeDoOffset(){
    long seen = tuplesSeen.getAndIncrement();
    if (seen % 10 == 0 && offsetStorage != null && fp.supportsOffsetManagement()){
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
  
  public String toString(){
    StringBuilder sb  = new StringBuilder();
    sb.append("Feed Partition "+fp.getPartitionId()+" " );
    sb.append("driver node "+ this.driverNode.toString());
    return sb.toString();
  }

  public boolean getGoOn() {
    return goOn.get();
  }

  public void setGoOn(boolean goOn) {
    this.goOn.set(goOn);
  }
  
  
  
}