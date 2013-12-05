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
package io.teknek.kafka;

import kafka.admin.CreateTopicCommand;

public class KafkaUtil {

  public static void createTopic(String name, int replica, int partitions, String zkConnectionString) {
    String[] arguments = new String[8];
    arguments[0] = "--zookeeper";
    arguments[1] = zkConnectionString;
    arguments[2] = "--replica";
    arguments[3] = replica+"";
    arguments[4] = "--partition";
    arguments[5] = partitions+"";
    arguments[6] = "--topic";
    arguments[7] = name;

    CreateTopicCommand.main(arguments);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
}
