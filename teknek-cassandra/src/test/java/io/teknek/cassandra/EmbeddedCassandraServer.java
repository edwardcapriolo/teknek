package io.teknek.cassandra;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;

import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;



public class EmbeddedCassandraServer {

  static Object started;
  static Cluster cluster; 
  
  public static final String KEYSPACE = "testing";
  public static final String COLUMNFAMILY = "tesfcf";
   
  @BeforeClass 
  public static void embeddedCassandrSetup() throws TTransportException, IOException, InterruptedException, ConfigurationException{
    if (started == null) {
      started = new Object();
      EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml");
      CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator("localhost:9157");
      cluster = HFactory.getOrCreateCluster("unit", cassandraHostConfigurator);
      KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition(KEYSPACE);
      cluster.addKeyspace(ksDef, true);
      ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE, COLUMNFAMILY);
      cluster.addColumnFamily(cfDef, true);
    }
  }
}
