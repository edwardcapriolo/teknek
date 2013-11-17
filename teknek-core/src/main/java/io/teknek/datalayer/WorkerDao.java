package io.teknek.datalayer;

import io.teknek.daemon.TechniqueDaemon;
import io.teknek.daemon.WorkerStatus;
import io.teknek.plan.Plan;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
  public static final String BASE_ZK = "/technique";
  /**
   * ephemeral nodes for worker registration live here
   */
  public static final String WORKERS_ZK = BASE_ZK + "/workers";
  /**
   * plans of stuff for workers to do live here
   */
  public static final String PLANS_ZK = BASE_ZK + "/plans";
  
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
    } catch (KeeperException  | InterruptedException e) {
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
  
  public static void createEphemeralNodeForDaemon(ZooKeeper zk, TechniqueDaemon d) throws WorkerDaoException {
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
}
