package technique.datalayer;

import kafka.admin.CreateTopicCommand;

public class KafkaUtil {

  public static void createTopic(String name, int replica, int partitions, String zkConnectionString) {
    String[] arguments = new String[8];
    arguments[0] = "--zookeeper";
    arguments[1] = zkConnectionString+" ";
    arguments[2] = "--replica";
    arguments[3] = replica+" ";
    arguments[4] = "--partition";
    arguments[5] = partitions+" ";
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
