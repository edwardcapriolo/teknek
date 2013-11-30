package io.teknek.zookeeper;

import java.io.IOException;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import io.teknek.feed.FeedPartition;
import io.teknek.offsetstorage.Offset;
import io.teknek.offsetstorage.OffsetStorage;
import io.teknek.plan.Plan;

public class ZookeeperOffsetStorage extends OffsetStorage {
  
  public static final String TEKNEK_ROOT = "/teknek";

  public static final String TEKNEK_OFFSET = TEKNEK_ROOT + "/offset";

  public static final String ZK_CONNECT = "zookeeper.connect";
  
  public ZookeeperOffsetStorage(FeedPartition feedPartition, Plan plan, Map<String,String> properties) {
    super(feedPartition, plan, properties);
  }
  
  @Override
  public void persistOffset(Offset o) {
    createZookeeperBase();
    ZooKeeper zk = null;
    String s = TEKNEK_OFFSET + "/" + plan.getName() + "-" + feedPartiton.getFeed().getName()+ "-" + feedPartiton.getPartitionId();
    try {
      zk = new ZooKeeper(properties.get(ZK_CONNECT), 100, new DummyWatcher());
      Stat stat = zk.exists(s, true);
      if (stat != null) {
        zk.setData(s, o.serialize(), stat.getVersion());
      } else {
        zk.create(s, o.serialize(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      zk.close();
    } catch (IOException | KeeperException | InterruptedException e1) {
      throw new RuntimeException(e1);
    }
  }
 
  @Override
  public Offset getCurrentOffset(){
      ZookeeperOffset zko = new ZookeeperOffset(feedPartiton.getOffset().getBytes());
      return zko;
  }

  @Override
  public Offset findLatestPersistedOffset() {
    String s = TEKNEK_OFFSET + "/" + plan.getName() + "-" + feedPartiton.getFeed().getName()+ "-" + feedPartiton.getPartitionId();
    ZooKeeper zk = null;
    try {
      zk = new ZooKeeper(properties.get(ZK_CONNECT), 100, new DummyWatcher());
      Stat stat = zk.exists(s, true);
      
      if (stat != null) {
        byte [] bytes = zk.getData(s, true, stat);
        ZookeeperOffset zo = new ZookeeperOffset(bytes);
        zk.close();
        return zo;
      } else {
        return null;
      }
    } catch (IOException | KeeperException | InterruptedException e1) {
      throw new RuntimeException(e1);
    }
  }

  public void createZookeeperBase() {
    try {
      ZooKeeper zk = new ZooKeeper(properties.get(ZK_CONNECT), 100, new DummyWatcher());
      if (zk.exists(TEKNEK_ROOT, true) == null) {
        zk.create(TEKNEK_ROOT, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (zk.exists(TEKNEK_OFFSET, true) == null) {
        zk.create(TEKNEK_OFFSET, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      zk.close();
    } catch (KeeperException  | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    } 
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
