package io.teknek.daemon;

import io.teknek.collector.CollectorProcessor;
import io.teknek.datalayer.WorkerDao;
import io.teknek.driver.Driver;
import io.teknek.driver.DriverFactory;
import io.teknek.driver.DriverNode;
import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.model.Operator;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.annotations.VisibleForTesting;


public class WorkerThread implements Watcher {
  private Plan plan;
  private List<String> otherWorkers;
  private TechniqueDaemon parent;
  private ZooKeeper zk;
  private Driver driver;
  
  public WorkerThread(Plan plan, List<String> otherWorkers, TechniqueDaemon parent){
    this.plan = plan;
    this.otherWorkers = otherWorkers;
    this.parent = parent;
  }
  
  /**
   * Deterine what partitions of the feed are already attached to other workers. 
   */
  public void init(){
    try {
      zk = new ZooKeeper(parent.getProperties().get(TechniqueDaemon.ZK_SERVER_LIST).toString(), 100, this);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    Feed feed = Feed.buildFeed(plan.getFeedDesc());
    List<WorkerStatus> workerStatus = WorkerDao.findAllWorkerStatusForPlan(zk, plan, otherWorkers);
    FeedPartition toProcess = findPartitionToProcess(workerStatus, feed.getFeedPartitions());

    if (toProcess != null){
      driver = DriverFactory.createDriver(toProcess, plan);
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
  
  @Override
  public void process(WatchedEvent event) {
    // TODO Auto-generated method stub
    
  }
}
