package io.teknek.daemon;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Starter is the kick-off/Driver program of the teknek-daemon.
 * Because we wish teknek to be simple to run out of the box we
 * also optionally start other embedded services with the daemon
 * 
 * For now this class lives here, but in the end we probably want 
 * an uber jar project, because we can cleanly separate out things
 * not needed in the core code.
 * 
 * @author edward
 *
 */
public class Starter {

  public static final String EMBED_ZK = "starter.embeddedzookeeper";
  public static final String EMBED_ZK_LOG = "starter.embeddedzookeeper.log";
  public static final String EMBED_ZK_SNAP = "starter.embeddedzookeeper.snap";
  
  public static final String EMBED_ZK_LOG_DIR = "/tmp/zklog";
  public static final String EMBED_ZK_SNAP_DIR = "/tmp/zksnap";
  
  final static Logger LOGGER = Logger.getLogger(Starter.class.getName());
  private ZooKeeperServer zk;
  
  public static void main(String [] args) throws IOException, InterruptedException{
    Starter starter = new Starter();
    starter.start();
  }
  
  public void start() {
    Properties props = new Properties();
    InputStream inputStream = this.getClass().getClassLoader()
            .getResourceAsStream("teknek.properties"); 
    try {
      if (inputStream != null){
        props.load(inputStream);
      }
    } catch (IOException e) {
      LOGGER.debug("loading props file "+ e);
    }
    if (props.getProperty(EMBED_ZK) != null) {
      maybeStartEmbdeddedZk(props);
    }
  }
  
  public void maybeStartEmbdeddedZk(Properties props) {
    if (Boolean.valueOf(props.getProperty(EMBED_ZK))){
      LOGGER.info("Starting embedded zookeeper");
      LOGGER.info("This is only appropriate for single node deployments");
      String logDir = props.getProperty(Starter.EMBED_ZK_LOG);
      if (logDir == null){
        LOGGER.info(Starter.EMBED_ZK_LOG+" was null. Using " + EMBED_ZK_LOG_DIR
                +" which is a VERY BAD PLACE if you want to keep this data.");
        logDir = EMBED_ZK_LOG_DIR;
      }
      String snapDir = props.getProperty(Starter.EMBED_ZK_SNAP_DIR);
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
        factory.configure(new InetSocketAddress("localhost", 2181), 1024);
        factory.startup(zk);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("count not start zk "+ e);
      }

    }
  }
}

  
  