package io.teknek.cassandra;

import java.nio.ByteBuffer;
import java.util.Map;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
  
public class CassandraOperator extends Operator {

  public static final String KEYSPACE = "cassandra.operator.keyspace";
  public static final String COLUMN_FAMILY = "cassandra.operator.columnfamily";
  public static final String HOST_LIST = "cassandra.operator.hostlist";
  public static final String ROW_KEY = "cassandra.operator.rowkey";
  public static final String COLUMN = "cassandra.operator.column";
  public static final String VALUE = "cassandra.operator.value";
  public static final String TIMESTAMP = "cassandra.operator.timestamp";
  public static final String PORT = "cassandra.operator.port";
  
  protected String clusterName = "TestCluster";
  protected Keyspace keyspace;
  
  @Override
  public void setProperties(Map<String, Object> properties) {
    super.setProperties(properties);
    AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
    .forCluster("ClusterName")
    .forKeyspace((String) properties.get(KEYSPACE))
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
    )
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
        .setPort((int)properties.get(PORT))
        .setMaxConnsPerHost(1)
        .setSeeds((String) properties.get(HOST_LIST))
    )
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance() );
    context.start();
    
    keyspace = context.getEntity(); 
  }
  
  @Override
  public void handleTuple(ITuple tuple) {
    MutationBatch m = keyspace.prepareMutationBatch();
    m.withRow(new ColumnFamily((String) properties.get(COLUMN_FAMILY),
            ByteBufferSerializer.get(),
            ByteBufferSerializer.get()), 
            (ByteBuffer) tuple.getField(ROW_KEY))
    .putColumn((ByteBuffer) tuple.getField(COLUMN), (ByteBuffer) tuple.getField(VALUE));
    try {
      OperationResult<Void> result = m.execute();
    } catch (ConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }    
  }

}