package io.teknek.streamstack;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

public class StandAloneCassandraServer {
  
  final static Logger LOGGER = Logger.getLogger(StandAloneCassandraServer.class.getName());
  public StandAloneCassandraServer() {

  }

  public void start() { 
    EmbeddedCassandraServerHelper e = new EmbeddedCassandraServerHelper();
    try {
      e.startEmbeddedCassandra("/cassandra.yaml");
    } catch (ConfigurationException | TTransportException | IOException e1) {
      LOGGER.error(e1);
    }
  }

  public static void main(String[] args) {
    StandAloneCassandraServer s = new StandAloneCassandraServer();
    s.start();
  }

}
