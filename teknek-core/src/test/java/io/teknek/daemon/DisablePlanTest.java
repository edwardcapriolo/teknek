package io.teknek.daemon;

import java.util.Properties;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

public class DisablePlanTest extends EmbeddedZooKeeperServer {

  static TeknekDaemon td = null;

  @BeforeClass
  public static void setup() {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getConnectString());
    td = new TeknekDaemon(props);
    td.setRescanMillis(1000);
    td.init();
  }

  @Test
  public void hangAround() {
    Plan p = new Plan().withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            10))).withRootOperator(new OperatorDesc(new TenSecondOperator()));
    p.setName("shutup");
    p.setMaxWorkers(1);
    p.setMaxWorkers(3);
    td.applyPlan(p);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Assert.assertNotNull( td.workerThreads );
    Assert.assertNotNull( td.workerThreads.get(p) );
    Assert.assertEquals( 1, td.workerThreads.get(p).size() );
  }

  @AfterClass
  public static void shutdown() {
    td.stop();
  }

}
