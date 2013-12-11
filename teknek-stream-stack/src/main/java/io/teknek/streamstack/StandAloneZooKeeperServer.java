package io.teknek.streamstack;

import io.teknek.daemon.Starter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class StandAloneZooKeeperServer {
  
  public static final String EMBED_ZK = "starter.embeddedzookeeper";
  public static final String EMBED_ZK_LOG = "starter.embeddedzookeeper.log";
  public static final String EMBED_ZK_SNAP = "starter.embeddedzookeeper.snap";
  
  public static final String EMBED_ZK_LOG_DIR = "./zklog";
  public static final String EMBED_ZK_SNAP_DIR = "./zksnap";
  
  final static Logger LOGGER = Logger.getLogger(Starter.class.getName());
  private ZooKeeperServer zk;
  
  private Properties props;
  
  public StandAloneZooKeeperServer(Properties props){
    this.props = props;
  }
  
  public static void main (String [] args){
    Properties props = new Properties();
    InputStream inputStream = StandAloneZooKeeperServer.class.getClassLoader()
            .getResourceAsStream("teknek.properties"); 
    try {
      if (inputStream != null){
        props.load(inputStream);
      }
    } catch (IOException e) {
      LOGGER.debug("loading props file "+ e);
    }
    
    if (props.getProperty(EMBED_ZK) != null) {
      StandAloneZooKeeperServer s = new StandAloneZooKeeperServer(props);
      s.start();
    } else {
      LOGGER.error(EMBED_ZK + " not specified. Will not start");
    }
  }
   
  public void start(){
    LOGGER.info("Starting embedded zookeeper");
    LOGGER.info("This is only appropriate for single node deployments");
    String logDir = props.getProperty(Starter.EMBED_ZK_LOG);
    if (logDir == null){
      LOGGER.info(Starter.EMBED_ZK_LOG+" was null. Using " + EMBED_ZK_LOG_DIR
              +" which is a VERY BAD PLACE if you want to keep this data.");
      logDir = EMBED_ZK_LOG_DIR;
    }
    String snapDir = props.getProperty(Starter.EMBED_ZK_SNAP);
    if (snapDir == null){
      LOGGER.info(Starter.EMBED_ZK_SNAP+" was null. Using " + EMBED_ZK_SNAP_DIR
              +" which is a VERY BAD PLACE if you want to keep this data.");
      snapDir = EMBED_ZK_SNAP_DIR;
    }
    File sn = new File(snapDir);
    sn.mkdir();
    File lg = new File(logDir);
    lg.mkdir();
    try {
      zk = new ZooKeeperServer(sn, lg, 3000);
      NIOServerCnxnFactory factory = new NIOServerCnxnFactory();
      //TODO hard codes need to be removed
      String host = "localhost";
      int port = 2181;
      factory.configure(new InetSocketAddress(host, port), 1024);
      LOGGER.info("starting single node zookeeper server on "+host+" "+port );
      factory.startup(zk);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("count not start zk "+ e);
    }

  }
}
