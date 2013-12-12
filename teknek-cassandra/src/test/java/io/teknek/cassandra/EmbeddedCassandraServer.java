package io.teknek.cassandra;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;

import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


   
public class EmbeddedCassandraServer {

  static Object started;

  public static final String KEYSPACE = "testing";

  public static final String COLUMNFAMILY = "tesfcf";
  
  protected static AstyanaxContext<Keyspace> context;
  
  protected static Keyspace keyspace;

  @BeforeClass
  public static void embeddedCassandrSetup() throws TTransportException, IOException,
          InterruptedException, ConfigurationException {
    if (started == null) {
      started = new Object();
      EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml");
      /*
       * CassandraHostConfigurator cassandraHostConfigurator = new
       * CassandraHostConfigurator("localhost:9157"); cluster = HFactory.getOrCreateCluster("unit",
       * cassandraHostConfigurator); KeyspaceDefinition ksDef =
       * HFactory.createKeyspaceDefinition(KEYSPACE); cluster.addKeyspace(ksDef, true);
       * ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE,
       * COLUMNFAMILY); cluster.addColumnFamily(cfDef, true);
       */

        context = new AstyanaxContext.Builder()
              .forCluster("ClusterName")
              .forKeyspace(KEYSPACE)
              .withAstyanaxConfiguration(
                      new AstyanaxConfigurationImpl()
                              .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
              .withConnectionPoolConfiguration(
                      new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9157)
                              .setMaxConnsPerHost(1).setSeeds("localhost:9157"))
              .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
              .buildKeyspace(ThriftFamilyFactory.getInstance());
      context.start();

      keyspace = context.getEntity();

      try {
        keyspace.createKeyspace(ImmutableMap
                .<String, Object> builder()
                .put("strategy_options",
                        ImmutableMap.<String, Object> builder().put("replication_factor", "1")
                                .build()).put("strategy_class", "SimpleStrategy").build());
        
        ColumnFamily<Byte, Byte> CF_STANDARD1 = ColumnFamily
                .newColumnFamily(COLUMNFAMILY, ByteSerializer.get(),
                        ByteSerializer.get());
        keyspace.createColumnFamily(CF_STANDARD1, null);
        
      } catch (ConnectionException e) {
        e.printStackTrace();
      }

    }
  }
}
