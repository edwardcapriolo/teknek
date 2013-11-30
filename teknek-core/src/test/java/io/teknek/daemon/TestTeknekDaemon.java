/*
Copyright 2013 Edward Capriolo, Matt Landolf, Lodwin Cueto

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.teknek.daemon;

import io.teknek.daemon.TeknekDaemon;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestTeknekDaemon extends EmbeddedZooKeeperServer {

  static TeknekDaemon td = null;
  
  @BeforeClass
  public static void setup(){
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getConnectString());
    td = new TeknekDaemon(props);
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
