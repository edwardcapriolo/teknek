package io.teknek.zookeeper;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import io.teknek.feed.FeedPartition;
import io.teknek.feed.FixedFeed;
import io.teknek.feed.TestFixedFeed;
import io.teknek.model.Tuple;
import io.teknek.plan.TestPlan;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.ZookeeperOffset;
import io.teknek.zookeeper.ZookeeperOffsetStorage;

public class TestZookeeperOffsetStorage extends EmbeddedZooKeeperServer {
   
  @Test
  public void test() {
    Map props = MapBuilder.makeMap(ZookeeperOffsetStorage.ZK_CONNECT, zookeeperTestServer.getConnectString());
    FixedFeed pf = new FixedFeed(TestFixedFeed.buildFeedProps());
    List<FeedPartition> parts = pf.getFeedPartitions();
    
    ZookeeperOffsetStorage zos = new ZookeeperOffsetStorage(parts.get(0),TestPlan.getPlan(), props);

    Assert.assertEquals("0", parts.get(0).getOffset());
    Tuple t = new Tuple();
    parts.get(0).next(t);
    Assert.assertEquals("1", parts.get(0).getOffset());
    ZookeeperOffset off = (ZookeeperOffset) zos.getCurrentOffset();
    Assert.assertEquals("1", new String(off.serialize()));
    zos.persistOffset(off);

    ZookeeperOffset fromZk = (ZookeeperOffset) zos.getCurrentOffset();
    Assert.assertEquals("1", new String(fromZk.serialize()));

    parts.get(0).next(t);
    zos.persistOffset(zos.getCurrentOffset());
    Assert.assertEquals("2", new String(((ZookeeperOffset) zos.getCurrentOffset()).serialize()));
  }
}
