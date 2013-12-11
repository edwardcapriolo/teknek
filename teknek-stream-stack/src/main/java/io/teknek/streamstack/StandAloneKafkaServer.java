package io.teknek.streamstack;

import io.teknek.daemon.Starter;

import java.io.File;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import org.apache.log4j.Logger;

public class StandAloneKafkaServer {
  
  final static Logger LOGGER = Logger.getLogger(StandAloneKafkaServer.class.getName());
  
  public static final String EMBED_KAFKA = "starter.embeddedkafka";
  public static final String EMBED_KAFKA_LOG = "starter.embeddedkafka.log";
  public static final String EMBED_KAFKA_LOG_DIR = "./target/kflog";
  
 
  public static KafkaServer server;
  private Properties props;
  public Properties brokerProps;
  
  public StandAloneKafkaServer(Properties props){
    this.props = props;
  }
  
  public static void main (String [] args){
    Properties props = PropertiesLoader.getProps();
    StandAloneKafkaServer s = new StandAloneKafkaServer(props);
    s.start();
  } 
  
  public void start(){
    String logDir = props.getProperty(EMBED_KAFKA_LOG);
    if (logDir == null){
      LOGGER.info(EMBED_KAFKA_LOG+" was null. Using " + EMBED_KAFKA_LOG_DIR
              +" which is a VERY BAD PLACE if you want to keep this data.");
      logDir = EMBED_KAFKA_LOG_DIR;
    }
    File ks1logdir = new File(logDir);
    if (!ks1logdir.exists()){
      ks1logdir.mkdir();
    }    
    brokerProps = new Properties();
    brokerProps.put("enable.zookeeper","true");
    brokerProps.put("zookeeper.connect", "localhost:2181");
    brokerProps.put("broker.id", "999");
    brokerProps.put("port","9092");
    KafkaConfig config = new KafkaConfig(brokerProps);
    server = new kafka.server.KafkaServer(config, new TimeImpl());
    server.startup();
    
  }
}


/** This is a copy if we get the deps correct we should not need this*/
class TimeImpl implements Time { 

  public TimeImpl(){
    
  }

  @Override
  public long milliseconds() {
    return System.currentTimeMillis();
  }

  @Override
  public long nanoseconds() {
    return System.nanoTime();
  }

  @Override
  public void sleep(long arg0) {
    try {
      Thread.sleep(arg0);
    } catch (Exception ex){}
  }
  
};


