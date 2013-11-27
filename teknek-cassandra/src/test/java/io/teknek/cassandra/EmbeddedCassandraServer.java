package io.teknek.cassandra;

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;

import io.teknek.kafka.EmbeddedKafkaServer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.testutils.EmbeddedServerHelper;

public class EmbeddedCassandraServer extends EmbeddedKafkaServer {

  static EmbeddedServerHelper embedded;
  static Cluster cluster; 
  
  public static final String KEYSPACE = "testing";
  public static final String COLUMNFAMILY = "tesfcf";
  
  @BeforeClass
  public static void embeddedCassandrSetup() throws TTransportException, IOException, InterruptedException, ConfigurationException{
    if (embedded == null) {
      embedded = new EmbeddedServerHelper();
      embedded.setup();
    }
    CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator("localhost:9157");
    cluster = HFactory.getOrCreateCluster("unit", cassandraHostConfigurator);
    KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition(KEYSPACE);
    cluster.addKeyspace(ksDef, true);
    ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE, COLUMNFAMILY);
    cluster.addColumnFamily(cfDef, true);
  }
  
}
