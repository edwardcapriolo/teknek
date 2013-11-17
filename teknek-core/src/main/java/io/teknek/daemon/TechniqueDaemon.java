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
import io.teknek.plan.Plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.WriteLock;

import com.google.common.annotations.VisibleForTesting;


public class TechniqueDaemon implements Watcher{

  public static final String ZK_SERVER_LIST = "technique.zk.servers";
  final static Logger logger = Logger.getLogger(TechniqueDaemon.class.getName());
  private int threadPoolSize = 4;
  private UUID myId;
  private Map<String,String> properties;
  private ExecutorService executor;
  private ZooKeeper zk;
  private long rescanMillis = 2000;
  private Map<Plan, List<Worker>> workerThreads;
  private boolean goOn = true;
  
  public TechniqueDaemon(Map<String,String> properties){
    myId = UUID.randomUUID();
    this.properties = properties;
    workerThreads = new HashMap<Plan,List<Worker>>();
  }
  
  
  public void init() {
    logger.debug("my UUID" + myId);
    executor = Executors.newFixedThreadPool(threadPoolSize);
    try {
      zk = new ZooKeeper(properties.get(ZK_SERVER_LIST).toString(), 100, this);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
    try {
      WorkerDao.createZookeeperBase(zk);
      WorkerDao.createEphemeralNodeForDaemon(zk, this);
    } catch (WorkerDaoException e) {
      throw new RuntimeException(e);
    }
      
    new Thread(){
      public void run(){
        while (goOn){
          try {
            List<String> children = WorkerDao.finalAllPlanNames(zk);
            logger.debug("Children found in zk" + children);
            for (String child: children){
              considerStarting(child);
            }
            Thread.sleep(rescanMillis);
          } catch (Exception ex){
            logger.error("Exception during scan "+ex);
            ex.printStackTrace();
          }
        }
      }
    }.start();
  }

  @VisibleForTesting
  public void applyPlan(Plan plan){
    try {
      WorkerDao.createOrUpdatePlan(plan, zk);
    } catch (WorkerDaoException e) {
      e.printStackTrace();
    }
  }
  
  private void considerStarting(String child){
    Plan plan = null;
    try {
      plan = WorkerDao.findPlanByName(zk, child);
    } catch (WorkerDaoException e) {
      logger.error(e);
    }
    if (plan == null){
      logger.error("did not find plan");
      return;
    }
    WriteLock l = new WriteLock(zk, WorkerDao.PLANS_ZK + "/"+ plan.getName(), null);
    try {
      boolean gotLock = l.lock();
      if (!gotLock){
        return;
      }
      List<String> children = WorkerDao.findWorkersWorkingOnPlan(zk, plan);
      int FUDGE_FOR_LOCK = 1;
      if (children.size()  >= plan.getMaxWorkers() + FUDGE_FOR_LOCK){
        logger.debug("already running max children:"+children.size()+" planmax"+plan.getMaxWorkers());
        logger.debug("already running max children:"+children.size()+" planmax"+plan.getMaxWorkers()+" running:"+children);
        
        return;
      }
      Worker worker = new Worker(plan, children, this);
      worker.init();
      worker.start();
      addWorkerToList(plan, worker);
      
    } catch (KeeperException | InterruptedException | WorkerDaoException e) {
      logger.warn("getting lock", e); 
    } finally {
      l.unlock();
    }
  }
  
  private void addWorkerToList(Plan plan, Worker worker) {
    List<Worker> list = workerThreads.get(plan);
    if (list == null){
      list = new ArrayList<Worker>();
    }
    list.add(worker);
    workerThreads.put(plan, list);
  }


  @Override
  public void process(WatchedEvent event) {
    // TODO Auto-generated method stub    
  }

  public UUID getMyId() {
    return myId;
  }

  public void setMyId(UUID myId) {
    this.myId = myId;
  }
 
  public void stop(){
    this.goOn = false;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
  
}