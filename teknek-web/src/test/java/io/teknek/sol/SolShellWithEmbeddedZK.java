package io.teknek.sol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Ignore;
import org.junit.Test;

import io.teknek.zookeeper.DummyWatcher;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

public class SolShellWithEmbeddedZK extends EmbeddedZooKeeperServer {

  //@Test
  /* copied the Sol main this is a bit ugly but allows us to test rapidly */
  //public void testToIgnore() throws IOException{
  public static void main (String [] args) throws Exception{
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    Sol s = new Sol();
    SolShellWithEmbeddedZK szk = new SolShellWithEmbeddedZK();
    szk.setupA();
    System.out.println(EmbeddedZooKeeperServer.zookeeperTestServer.getConnectString());
    ZooKeeper zk = new ZooKeeper(EmbeddedZooKeeperServer.zookeeperTestServer.getConnectString(), 100, new DummyWatcher());
    s.setZookeeper(zk);
    String line = null;
    System.out.print(Sol.rootPrompt);
    while ((line = br.readLine()) != null) {
      SolReturn ret = s.send(line);
      if (ret.getMessage().length()>0){
        System.out.println(ret.getMessage());
      }
      System.out.print(ret.getPrompt());
    }
  }
}
