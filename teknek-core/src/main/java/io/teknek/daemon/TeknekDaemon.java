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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;

import com.google.common.annotations.VisibleForTesting;

public class TeknekDaemon implements Watcher{

  public static final String ZK_SERVER_LIST = "teknek.zk.servers";
  public static final String MAX_WORKERS = "teknek.max.workers";
  final static Logger logger = Logger.getLogger(TeknekDaemon.class.getName());
  private int maxWorkers = 4;
  private UUID myId;
  private Properties properties;
  private ZooKeeper zk;
  private long rescanMillis = 5000;
  ConcurrentHashMap<Plan, List<Worker>> workerThreads;
  private boolean goOn = true;
  
  public TeknekDaemon(Properties properties){
    
    myId = UUID.randomUUID();
    this.properties = properties;
    workerThreads = new ConcurrentHashMap<Plan,List<Worker>>();
    if (properties.containsKey(MAX_WORKERS)){
      maxWorkers = Integer.parseInt(properties.getProperty(MAX_WORKERS));
    }
  }
  
  public void init() {
    logger.debug("my UUID" + myId);
    System.out.println("connecting to "+properties.getProperty(ZK_SERVER_LIST));
    try {
      zk = new ZooKeeper(properties.getProperty(ZK_SERVER_LIST), 100, this);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        /*
         * Session establishment is asynchronous. This constructor will initiate connection to the server and return immediately - potentially (usually) before the session is fully established. The watcher argument specifies the watcher that will be notified of any changes in state. This notification can come at any point before or after the constructor call has returned. 
         */
        e.printStackTrace();
      }
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
            if (workerThreads.size() < maxWorkers) {
              List<String> children = WorkerDao.finalAllPlanNames(zk);  
              logger.debug("List of plans: " + children);
              for (String child: children){
                considerStarting(child);
              }
            } else {
              logger.debug("Will not attemt to start worker. Already at max workers "+ workerThreads.size());
            }
           
          } catch (Exception ex){
            logger.error("Exception during scan "+ex);
            ex.printStackTrace();
          }
          try {
            Thread.sleep(rescanMillis);
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
  
  @VisibleForTesting
  public void deletePlan(Plan plan){
    try {
      WorkerDao.deletePlan(zk, plan);
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
      return;
    }
    if (plan == null){
      logger.error("did not find plan");
      return;
    }
    if (plan.isDisabled()){
      logger.debug("disabled "+ plan.getName());
      return;
    }
    logger.debug("trying to acqure lock on " + WorkerDao.LOCKS_ZK + "/"+ plan.getName());
    try {
      WorkerDao.maybeCreatePlanLockDir(zk, plan);
    } catch (WorkerDaoException e1) {
      logger.error(e1);
      return;
    }
    final CountDownLatch c = new CountDownLatch(1);
    WriteLock l = new WriteLock(zk, WorkerDao.LOCKS_ZK + "/"+ plan.getName(), null);
    l.setLockListener(new LockListener(){

      @Override
      public void lockAcquired() {
        logger.debug(myId + " counting down");
        c.countDown();
      }

      @Override
      public void lockReleased() {
        logger.debug(myId + " released");
      }
      
    });
    try {
      boolean gotLock = l.lock(); 
      /*
      if (!gotLock){
        logger.debug("did not get lock");
        return;
      }*/
      boolean hasLatch = c.await(3000, TimeUnit.MILLISECONDS);
      if (hasLatch){
        /* plan could have been disabled after latch */
        try {
          plan = WorkerDao.findPlanByName(zk, child);
        } catch (WorkerDaoException e) {
          logger.error(e);
        }
        if (plan.isDisabled()){
          logger.debug("disabled "+ plan.getName());
        } else {
          List<String> children = WorkerDao.findWorkersWorkingOnPlan(zk, plan);
          if (children.size() >= plan.getMaxWorkers()) {
            logger.debug("already running max children:" + children.size() + " planmax:"
                    + plan.getMaxWorkers() + " running:" + children);
          } else {
            logger.debug("starting worker");
            Worker worker = new Worker(plan, children, this);
            worker.init();
            worker.start();
            addWorkerToList(plan, worker);
          }
        }
      }
    } catch (KeeperException | InterruptedException | WorkerDaoException e) {
      logger.warn("getting lock", e); 
    } finally {
      try {
        l.unlock();
      } catch (RuntimeException ex){
        logger.debug(ex);
        ex.printStackTrace();
      }
    }
  }
  
  private void addWorkerToList(Plan plan, Worker worker) {
    logger.debug("adding worker " + worker.getMyId() + " to plan "+plan.getName());
    List<Worker> list = workerThreads.get(plan);
    if (list == null) {
      list = Collections.synchronizedList(new ArrayList<Worker>());
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

  public Properties getProperties() {
    return properties;
  }

  public long getRescanMillis() {
    return rescanMillis;
  }

  public void setRescanMillis(long rescanMillis) {
    this.rescanMillis = rescanMillis;
  }
  
}