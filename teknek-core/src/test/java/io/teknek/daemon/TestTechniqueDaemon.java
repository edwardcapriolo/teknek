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
