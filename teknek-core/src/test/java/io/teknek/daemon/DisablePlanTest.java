package io.teknek.daemon;

import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.test.WaitForCondition;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

public class DisablePlanTest extends EmbeddedZooKeeperServer {

  private TeknekDaemon td = null;

  @Before
  public void before() {
    System.out.println("Starting "+this.getClass().getSimpleName() );
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getConnectString());
    td = new TeknekDaemon(props);
    td.setRescanMillis(1000);
    td.init();
  }

  @Test
  public void hangAround() {
    final Plan p = new Plan().withFeedDesc(
            new FeedDesc().withFeedClass(FixedFeed.class.getName()).withProperties(
                    MapBuilder.makeMap(FixedFeed.NUMBER_OF_PARTITIONS, 2, FixedFeed.NUMBER_OF_ROWS,
                            100000))).withRootOperator(new OperatorDesc(new TenSecondOperator()));
    p.setName("shutup");
    p.setMaxWorkers(1);
    td.applyPlan(p);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
    }
    Assert.assertNotNull(td.workerThreads);
    System.out.println(td.workerThreads);
    Assert.assertNotNull(td.workerThreads.get(p));
    Assert.assertEquals(1, td.workerThreads.get(p).size());

    
    System.out.println("disabling");
    p.setDisabled(true);
    td.applyPlan(p);
    
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    Assert.assertNotNull(td.workerThreads.get(p));
    Assert.assertEquals(0, td.workerThreads.get(p).size());
    
  }

  @After
  public void after() {
    td.stop();
    System.out.println("Ending "+this.getClass().getSimpleName() );
  }

}
