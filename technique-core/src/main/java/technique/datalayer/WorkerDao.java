package technique.datalayer;

import java.io.IOException;
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

import technique.plan.Plan;

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
  public static void createZookeeperBase(ZooKeeper zk) throws KeeperException, InterruptedException {
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
  }
  
  /**
   * 
   * @param zk
   * @return a list of all plans stored in zk
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static List<String> finalAllPlanNames (ZooKeeper zk) throws KeeperException, InterruptedException {
    return zk.getChildren(PLANS_ZK, false);
  }
  
  public static Plan findPlanByName(ZooKeeper zk, String name) throws KeeperException, InterruptedException, JsonParseException, JsonMappingException, IOException{
    Stat s = zk.exists(PLANS_ZK + "/"+ name, false);
    byte[] b = zk.getData(PLANS_ZK + "/" + name, false, s);
    Plan plan = deserializePlan(b);
    return plan;
    
  }
  
  public static Plan deserializePlan(byte [] b) throws JsonParseException, JsonMappingException, IOException{
    ObjectMapper om = new ObjectMapper();
    Plan p1 = om.readValue(b, Plan.class);
    return p1;
  }
}
