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
package io.teknek.datalayer;

import io.teknek.daemon.TeknekDaemon;
import io.teknek.daemon.WorkerStatus;
import io.teknek.plan.Bundle;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This component deals with persistence into zk for the worker node
 * note: we likely want a custom exception here
 * @author edward
 *
 */
public class WorkerDao {

  final static Logger logger = Logger.getLogger(WorkerDao.class.getName());
  /**
   * Base directory of the entire application
   */
  public static final String BASE_ZK = "/teknek";
  /**
   * ephemeral nodes for worker registration live here
   */
  public static final String WORKERS_ZK = BASE_ZK + "/workers";
  /**
   * plans of stuff for workers to do live here
   */
  public static final String PLANS_ZK = BASE_ZK + "/plans";
  /**
   * saved feeds and operators
   */
  public static final String SAVED_ZK = BASE_ZK + "/saved";
  /**
   * Holds zk locks for chosing plans
   */
  public static final String LOCKS_ZK = BASE_ZK + "/locks";
  
  /**
   * Creates all the required base directories in ZK for the application to run 
   * @param zk
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void createZookeeperBase(ZooKeeper zk) throws WorkerDaoException {
    try {
      if (zk.exists(BASE_ZK, true) == null) {
        logger.info("Creating "+BASE_ZK+" heirarchy");
        zk.create(BASE_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (zk.exists(WORKERS_ZK, false) == null) {
        zk.create(WORKERS_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (zk.exists(PLANS_ZK, true) == null) {
        zk.create(PLANS_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (zk.exists(SAVED_ZK, false) == null) {
        zk.create(SAVED_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (zk.exists(LOCKS_ZK, false) == null) {
        zk.create(LOCKS_ZK, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException  | InterruptedException e) {
      e.printStackTrace();
      throw new WorkerDaoException(e);
    } 
  }
  
  public static List<String> findWorkersWorkingOnPlan(ZooKeeper zk, Plan p) throws WorkerDaoException{
    try {
      return zk.getChildren(PLANS_ZK + "/" + p.getName(), false);
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  /**
   * 
   * @param zk
   * @return a list of all plans stored in zk
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static List<String> finalAllPlanNames (ZooKeeper zk) throws WorkerDaoException {
    try {
      return zk.getChildren(PLANS_ZK, false);
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public static Plan findPlanByName(ZooKeeper zk, String name) throws WorkerDaoException {
    try {
      Stat s = zk.exists(PLANS_ZK + "/"+ name, false);
      byte[] b = zk.getData(PLANS_ZK + "/" + name, false, s);
      return deserializePlan(b);
    } catch (IOException | KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    } 
  }
  
  public static Plan deserializePlan(byte [] b) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    Plan p1 = om.readValue(b, Plan.class);
    return p1;
  }
  
  public static byte[] serializePlan(Plan plan) {
    ObjectMapper map = new ObjectMapper();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      map.writeValue(baos, plan);
    } catch (IOException ex) {
      logger.error(ex);
    }
    return baos.toByteArray();
  }
  
  
  public static void createOrUpdatePlan(Plan plan, ZooKeeper zk) throws WorkerDaoException {
      Stat s;
      try {
        WorkerDao.createZookeeperBase(zk);
        s = zk.exists(PLANS_ZK+ "/" + plan.getName(), false);
        if (s != null) {
          zk.setData(PLANS_ZK+ "/" + plan.getName(), serializePlan(plan), s.getVersion());
        } else {
          zk.create(PLANS_ZK+ "/" + plan.getName(), serializePlan(plan), Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT);
        }
      } catch (KeeperException | InterruptedException e) {
        throw new WorkerDaoException(e);
      }
  }
  
  public static void createEphemeralNodeForDaemon(ZooKeeper zk, TeknekDaemon d) throws WorkerDaoException {
    try {
      zk.create(WORKERS_ZK +"/"+d.getMyId().toString(), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public static List<WorkerStatus> findAllWorkerStatusForPlan(ZooKeeper zk, Plan plan, List<String> otherWorkers){
    List<WorkerStatus> results = new ArrayList<WorkerStatus>();
    for (String worker : otherWorkers) {
      String lookAtPath = PLANS_ZK + "/" + plan.getName() + "/" + worker;
      Stat stat = null;
      try {
        stat = zk.exists(lookAtPath, false);
        byte[] data = zk.getData(lookAtPath, false, stat);
        results.add(new WorkerStatus(worker, new String(data)));
      } catch (KeeperException | InterruptedException e) {
        logger.error(e);
      }
    }
    return results;
  }
  
  /**
   * Registers an ephemeral node representing ownership of a feed partition
   * @param zk
   * @param plan
   * @param s
   * @throws WorkerDaoException 
   */
  public static void registerWorkerStatus(ZooKeeper zk, Plan plan, WorkerStatus s) throws WorkerDaoException{
    String writeToPath = PLANS_ZK + "/" + plan.getName() + "/" + s.getWorkerUuid();
    try {
      zk.create(writeToPath, s.getFeedPartitionId().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      logger.debug("Registered as ephemeral "+ writeToPath);
      zk.exists(PLANS_ZK+ "/" + plan.getName(), true);
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public static FeedDesc loadSavedFeedDesc(ZooKeeper zk, String group, String name) throws WorkerDaoException {
    String readPath = SAVED_ZK + "/" + group + "-" + name + "-" + "feedDesc";
    try {
      Stat stat = zk.exists(readPath, false);
      if (stat != null){
        byte [] data = zk.getData(readPath, false, stat);
        return deserializeFeedDesc(data);
      } else {
        throw new WorkerDaoException("not found in zk");
      }
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public static OperatorDesc loadSavedOperatorDesc(ZooKeeper zk, String group, String name) throws WorkerDaoException{
    String readPath = SAVED_ZK + "/" + group + "-" + name + "-" + "operatorDesc";
    try {
      Stat stat = zk.exists(readPath, false);
      if (stat != null){
        byte [] data = zk.getData(readPath, false, stat);
        return deserializeOperatorDesc(data);
      } else {
        throw new WorkerDaoException("not found in zk");
      }
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public static void saveOperatorDesc(ZooKeeper zk, OperatorDesc desc, String group, String name)
          throws WorkerDaoException {
    String readPath = SAVED_ZK + "/" + group + "-" + name + "-" + "operatorDesc";
    createZookeeperBase(zk);
    try {
      String s = zk.create(readPath, serializeOperatorDesc(desc), Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }

  public static void saveFeedDesc(ZooKeeper zk, FeedDesc desc, String group, String name)
          throws WorkerDaoException {
    String readPath = SAVED_ZK + "/" + group + "-" + name + "-" + "feedDesc";
    createZookeeperBase(zk);
    try {
      String s = zk.create(readPath, serializeFeedDesc(desc), Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public static OperatorDesc deserializeOperatorDesc(byte [] b) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    OperatorDesc p1 = om.readValue(b, OperatorDesc.class);
    return p1;
  }
  
  public static FeedDesc deserializeFeedDesc(byte [] b) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    FeedDesc p1 = om.readValue(b, FeedDesc.class);
    return p1;
  }
  
  public static byte [] serializeFeedDesc(FeedDesc desc) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    byte [] b = om.writeValueAsBytes(desc);
    return b;
  }
  
  public static byte [] serializeOperatorDesc(OperatorDesc desc) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    byte [] b = om.writeValueAsBytes(desc);
    return b;
  }

  //TODO om can read streams
  //throw worker dao exceptions..
  public static Bundle getBundleFromUrl(URL u) {
    StringBuffer sb = new StringBuffer();
    try {
      URLConnection yc = u.openConnection();
      BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));

      String inputLine = null;
      while ((inputLine = in.readLine()) != null)
        sb.append(inputLine + "\n");
      in.close();
    } catch (IOException e) {
      logger.warn(e.getMessage());
    }
    ObjectMapper om = new ObjectMapper();
    Bundle b = null;
    try {
      b = om.readValue(sb.toString().getBytes(), Bundle.class);
    } catch ( IOException e) {
      logger.warn(e.getMessage());
      e.printStackTrace();
    }
    return b;
  }

  public static void saveBundle(ZooKeeper zk, Bundle b) throws WorkerDaoException {
    for (OperatorDesc o : b.getOperatorList() ){
      WorkerDao.saveOperatorDesc(zk, o, b.getPackageName(), o.getName());
    }
    for (FeedDesc f: b.getFeedDescList()){
      WorkerDao.saveFeedDesc(zk, f, b.getPackageName(), f.getName());
    }
  }
  
  public static void deletePlan(ZooKeeper zk, Plan p) throws WorkerDaoException {
    String planNode = PLANS_ZK + "/" + p.getName();
    try {
      Stat s = zk.exists(planNode, false);
      if (s != null) {
        zk.delete(planNode, s.getVersion());
      }
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
  
  public static void maybeCreatePlanLockDir(ZooKeeper zk, Plan plan) throws WorkerDaoException {
    try {
      String planLock = LOCKS_ZK + "/" + plan.getName();
      if (zk.exists(planLock, false) == null) {
        logger.debug("Creating " + planLock);
        zk.create(planLock, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException | InterruptedException e) {
      throw new WorkerDaoException(e);
    }
  }
}
