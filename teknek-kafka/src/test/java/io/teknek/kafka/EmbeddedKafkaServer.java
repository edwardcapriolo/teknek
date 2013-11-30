package io.teknek.kafka;

import io.teknek.zookeeper.EmbeddedZooKeeperServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import kafka.admin.CreateTopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.junit.BeforeClass;

import com.netflix.curator.test.TestingServer;

public class EmbeddedKafkaServer extends EmbeddedZooKeeperServer {

  public static KafkaServer server;  

  /**
   * This class is named setupB because we wanted it to run after
   * setupA. This method seems to order setup properly. Your mileage may vary.
   * @throws Exception
   */
  @BeforeClass
  public static void setupB() throws Exception{
    String kdir = "/tmp/ks1logdir";
    if (server != null){
      return;
    }    
    File ks1logdir = new File(kdir);
    if (ks1logdir.exists()){
      delete(ks1logdir);
    }
    ks1logdir.mkdir();
    Properties brokerProps= new Properties();
    brokerProps.put("enable.zookeeper","true");
    brokerProps.put( "broker.id", "1");
    putZkConnect(brokerProps, "localhost:"+zookeeperTestServer.getPort());
    brokerProps.put("port","9092");
    brokerProps.setProperty("num.partitions", "10");
    brokerProps.setProperty("log.dir", kdir);
    KafkaConfig config= new KafkaConfig(brokerProps);
    if (server == null) {
      server = new kafka.server.KafkaServer(config, new TimeImpl());
      server.startup();
    }  
  }
  
  public static void createTopic(String name, int replica, int partitions ) {
    String[] arguments = new String[8];
    arguments[0] = "--zookeeper";
    arguments[1] = "localhost:"+zookeeperTestServer.getPort();
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
    
  protected static ProducerConfig createProducerConfig(){
    Properties producerProps = new Properties();
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    putZkConnect(producerProps, "localhost:"+zookeeperTestServer.getPort());
    producerProps.setProperty("batch.size", "10");
    producerProps.setProperty("producer.type", "async");
    producerProps.put("metadata.broker.list", "localhost:9092");
    return new ProducerConfig(producerProps); 
  }

  protected ConsumerConfig createConsumerConfig(){
    Properties consumerProps = new Properties();
    putZkConnect(consumerProps, "localhost:"+zookeeperTestServer.getPort());
    putGroupId(consumerProps, "group1");
    consumerProps.put("auto.offset.reset", "smallest");
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
    return consumerConfig;
  }

  
  public static void delete(File f) throws IOException {
    if (!f.exists()) {
      return;
    }
    if (f.isDirectory()) {
      for (File c : f.listFiles()) {
        delete(c);
      }
    }
    if (!f.delete()) {
      throw new FileNotFoundException("Failed to delete file: " + f);
    }
  }
 
  
  public static void putZkConnect(Properties props, String zkconn) {
    props.put("zk.connect", zkconn);
    props.put("zookeeper.connect", zkconn);
  }

  public static void putGroupId(Properties props, String groupId) {
    props.put("groupid", groupId);
    props.put("group.id", groupId);
  }
  
}
