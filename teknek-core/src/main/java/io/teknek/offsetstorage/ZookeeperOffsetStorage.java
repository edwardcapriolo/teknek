package io.teknek.offsetstorage;

import java.io.IOException;
import java.util.GregorianCalendar;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import io.teknek.daemon.TechniqueDaemon;
import io.teknek.feed.FeedPartition;
import io.teknek.plan.Plan;

public class ZookeeperOffsetStorage extends OffsetStorage implements Watcher {
  
  public static final String TEKNEK_ROOT = "/teknek";

  public static final String TEKNEK_OFFSET = TEKNEK_ROOT + "/offset";

  public ZookeeperOffsetStorage(FeedPartition feedPartition, Plan plan) {
    super(feedPartition, plan);
  }
  
  @Override
  public void persistOffset(Offset o) {
    ZooKeeper zk = null;
    String s = TEKNEK_OFFSET + "/" + plan.getName() + "-" + part.getFeed().getName()+ "-" + part.getPartitionId();
    try {
      zk = new ZooKeeper("localhost", 100, this);
      Stat stat = zk.exists(s, false);
      if (stat != null) {
        zk.setData(s, o.serialize(), stat.getVersion() + 1);
      } else {
        zk.create(s, o.serialize(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      zk.close();
    } catch (IOException | KeeperException | InterruptedException e1) {
      throw new RuntimeException(e1);
    }
  }

  @Override
  public Offset getCurrentOffset() {
      ZookeeperOffset zko = new ZookeeperOffset(part.getOffset().getBytes());
      return zko;
  }

  @Override
  public Offset findLatestPersistedOffset() {
    String s = TEKNEK_OFFSET + "/" + plan.getName() + "-" + part.getFeed().getName()+ "-" + part.getPartitionId();
    ZooKeeper zk = null;
    try {
      zk = new ZooKeeper("localhost", 100, this);
      Stat stat = zk.exists(s, false);
      byte [] bytes = zk.getData(s, false, stat);
      ZookeeperOffset zo = new ZookeeperOffset(bytes);
      zk.close();
      return zo;   
    } catch (IOException | KeeperException | InterruptedException e1) {
      throw new RuntimeException(e1);
    }
  }


  @Override
  public void process(WatchedEvent event) {

  }

}

class ZookeeperOffset extends Offset {

  private String offset;
  
  public ZookeeperOffset(byte[] bytes) {
    super(bytes);
    offset = new String(bytes);
  }
  
  @Override
  public byte[] serialize() {
    return offset.getBytes();
  }
  
}
