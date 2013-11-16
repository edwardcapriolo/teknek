package io.teknek.daemon;

import io.teknek.daemon.TechniqueDaemon;
import io.teknek.kafka.EmbeddedKafkaServer;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestTechniqueDaemon extends EmbeddedKafkaServer {

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
    try {
      Thread.sleep(5000);
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
