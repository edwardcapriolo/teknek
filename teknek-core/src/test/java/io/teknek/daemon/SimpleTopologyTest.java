package io.teknek.daemon;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.teknek.driver.Minus1Operator;
import io.teknek.driver.Times2Operator;
import io.teknek.feed.FixedFeed;
import io.teknek.kafka.EmbeddedKafkaServer;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;

public class SimpleTopologyTest extends EmbeddedKafkaServer{

static TechniqueDaemon td = null;
  
  @BeforeClass
  public static void setup(){
    Map<String,String> props = new HashMap<String,String>();
    props.put(TechniqueDaemon.ZK_SERVER_LIST, zookeeperTestServer.getConnectString());
    td = new TechniqueDaemon(props);
    td.init();
  }
  @Test
  public void hangAround(){
    Plan p = new Plan().withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            10))).withRootOperator(
            new OperatorDesc(new BeLoudOperator()));
    p.setName("yell");
    p.setMaxWorkers(1);
    td.applyPlan(p);
    try {
      Thread.sleep(6000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @AfterClass
  public static void shutdown(){
    td.stop();
  }
}
