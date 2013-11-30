package io.teknek.zookeeper;

import org.junit.BeforeClass;

import com.netflix.curator.test.TestingServer;

public class EmbeddedZooKeeperServer {
  public static TestingServer zookeeperTestServer ;
  
  /**
   * This class is named setupA because later on when setting up
   * a kafka server we call that setupB. This method seems to order
   * setup properly. Your mileage may vary.
   * @throws Exception
   */
  @BeforeClass
  public static void setupA() throws Exception{
    if (zookeeperTestServer == null){
      zookeeperTestServer = new TestingServer();
    }
  }
}
