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
package io.teknek.daemon;

import io.teknek.datalayer.WorkerDao;
import io.teknek.datalayer.WorkerDaoException;
import io.teknek.driver.Driver;
import io.teknek.driver.DriverFactory;
import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.plan.Plan;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.annotations.VisibleForTesting;


public class Worker implements Watcher {
  
  final static Logger logger = Logger.getLogger(Worker.class.getName());
  private Plan plan;
  private List<String> otherWorkers;
  private TeknekDaemon parent;
  private ZooKeeper zk;
  private Driver driver;
  private UUID myId;
  private Thread driverThread;
  
  public Worker(Plan plan, List<String> otherWorkers, TeknekDaemon parent){
    this.plan = plan;
    this.otherWorkers = otherWorkers;
    this.parent = parent;
    myId = UUID.randomUUID();
  }
  
  /**
   * Deterine what partitions of the feed are already attached to other workers. 
   */
  public void init(){
    try {
      zk = new ZooKeeper(parent.getProperties().get(TeknekDaemon.ZK_SERVER_LIST).toString(), 100, this);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    Feed feed = DriverFactory.buildFeed(plan.getFeedDesc());
    List<WorkerStatus> workerStatus = WorkerDao.findAllWorkerStatusForPlan(zk, plan, otherWorkers);
    FeedPartition toProcess = findPartitionToProcess(workerStatus, feed.getFeedPartitions());
    if (toProcess != null){
      driver = DriverFactory.createDriver(toProcess, plan);
      driver.initialize();
      WorkerStatus iGotThis = new WorkerStatus(myId.toString(), toProcess.getPartitionId());
      try {
        WorkerDao.registerWorkerStatus(zk, plan, iGotThis);
        
      } catch (WorkerDaoException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException("Could not start plan "+plan.getName());
    }
  }

  public void start(){
    driverThread = new Thread(driver);
    driverThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        logger.warn("Thread died removing worker from list " + t, e );
        shutdown();
      }
    });
    driverThread.start();
  }
  /**
   * Remove ourselves from parents worker threads and close our zk connection
   */
  private void shutdown(){
    parent.workerThreads.get(plan).remove(this);
    if (zk != null) {
      try {
        logger.debug("closing " + zk.getSessionId());
        zk.close();
        zk = null;
      } catch (InterruptedException e1) {
        logger.debug(e1);
      }
      logger.debug("shutdown complete");
    }
  }
  /**
   * TODO: in here it "should" be impossible fro a null return
   * @param workerStatus
   * @param feedPartitions
   * @return a partition available for processing or null if none are available
   */
  @VisibleForTesting
  FeedPartition findPartitionToProcess(List<WorkerStatus> workerStatus, List<FeedPartition> feedPartitions){
    for (int i = 0; i < feedPartitions.size(); i++) {
      FeedPartition aPartition = feedPartitions.get(i);
      boolean partitionTaken = false;
      for (int j = 0; j < workerStatus.size(); j++) {
        if (workerStatus.get(j).getFeedPartitionId().equals(aPartition.getPartitionId())) {
          partitionTaken = true;
        }
      }
      if (partitionTaken == false){
        return aPartition;
      }
    }
    return null;
  }
  
  /*
   * We could response to a node change by attempting to modify the driver in the current worker
   * however it is cleaner to bring this worker down gracefully, and let it restart elsewhere
   * (non-Javadoc)
   * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  @Override
  public void process(WatchedEvent event) {
    logger.debug("recived event "+ event);
    if (event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeDeleted) {
      driver.setGoOn(false);
      shutdown();
      //wait for graceful termination
      //close zk
      //remove this class from parent list
    }
  }

  public UUID getMyId() {
    return myId;
  }
  
}
