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
 
public class CassandraBatchingOperator extends CassandraOperator {

  public static final String BATCH_SIZE = "cassandra.operator.batchsize";
  protected int batchSize = 1;
  private int count = 0;
  MutationBatch m ;
  
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
    batchSize = (Integer) properties.get(BATCH_SIZE);
    keyspace = context.getEntity();
    m = keyspace.prepareMutationBatch();
  }
 
  @Override
  public void handleTuple(ITuple tuple) {
    m.withRow(new ColumnFamily((String) properties.get(COLUMN_FAMILY),
            ByteBufferSerializer.get(),
            ByteBufferSerializer.get()), 
            (ByteBuffer) tuple.getField(ROW_KEY))
    .putColumn((ByteBuffer) tuple.getField(COLUMN), (ByteBuffer) tuple.getField(VALUE));
    count++;
    if (count % batchSize == 0){
      try {
        OperationResult<Void> result = m.execute();
      } catch (ConnectionException e) {
        e.printStackTrace();
      }
      m = keyspace.prepareMutationBatch();
    }
  }

}
