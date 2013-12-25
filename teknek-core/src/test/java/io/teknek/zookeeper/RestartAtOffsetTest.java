package io.teknek.zookeeper;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.teknek.daemon.BeLoudOperator;
import io.teknek.daemon.TeknekDaemon;
import io.teknek.datalayer.WorkerDao;
import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OffsetStorageDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.ZookeeperOffset;
import io.teknek.zookeeper.ZookeeperOffsetStorage;

public class RestartAtOffsetTest extends EmbeddedZooKeeperServer {

  TeknekDaemon td = null;
  Plan p;
  
  @Before
  public void setupD(){
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getConnectString());
    td = new TeknekDaemon(props);
    td.init();
  }
  
  public static Map<String,Object> buildFeedProps(){
    Map<String,Object> props = new HashMap<String,Object>();
    props.put(FixedFeed.NUMBER_OF_PARTITIONS, 1);
    props.put(FixedFeed.NUMBER_OF_ROWS, 100);
    return props;
  }
  
  @Test
  public void startAtOffset(){
    Map zkOffset = MapBuilder.makeMap(ZookeeperOffsetStorage.ZK_CONNECT, zookeeperTestServer.getConnectString());
    p = new Plan()
      .withOffsetStorageDesc(new OffsetStorageDesc()
        .withOperatorClass(ZookeeperOffsetStorage.class.getName())
          .withParameters(zkOffset))
      .withFeedDesc(new FeedDesc()
        .withFeedClass(FixedFeed.class.getName())
          .withProperties( buildFeedProps() ))
     .withRootOperator(new OperatorDesc(new BeLoudOperator()));
    p.setName("catsup");
    p.setMaxWorkers(1);
    
    FixedFeed pf = new FixedFeed(buildFeedProps());
    ZookeeperOffsetStorage zos = new ZookeeperOffsetStorage(pf.getFeedPartitions().get(0), p, zkOffset);
    ZookeeperOffset zo = new ZookeeperOffset("6".getBytes());
    zos.persistOffset(zo);
    
    td.applyPlan(p);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  @After
  public void shutdown(){
    td.deletePlan(p);
    td.stop();
  }
}
