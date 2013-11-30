package io.teknek.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * When you want to comminicate with zookeeper but you do not want to watch anything.
 * @author edward
 *
 */
public class DummyWatcher implements Watcher {

  @Override
  public void process(WatchedEvent event) {
  }

}
