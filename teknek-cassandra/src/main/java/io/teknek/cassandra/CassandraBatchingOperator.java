package io.teknek.cassandra;

import java.nio.ByteBuffer;
import java.util.Map;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import io.teknek.model.ITuple;

public class CassandraBatchingOperator extends CassandraOperator {

  public static final String BATCH_SIZE = "cassandra.operator.batchsize";
  Mutator<ByteBuffer> m;
  protected int batchSize = 1;
  private int count = 0;
  
  @Override
  public void setProperties(Map<String, Object> properties) {
    super.setProperties(properties);
    m = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
    batchSize = (Integer) properties.get(BATCH_SIZE);
  }

  @Override
  public void handleTuple(ITuple tuple) {
    HColumn col = HFactory.createColumn((ByteBuffer) tuple.getField(COLUMN), (ByteBuffer) tuple.getField(VALUE), System.nanoTime(), ByteBufferSerializer.get(), ByteBufferSerializer.get());
    m.addInsertion((ByteBuffer) tuple.getField(ROW_KEY), (String) properties.get(COLUMN_FAMILY), col);
    count++;
    if (count % batchSize == 0){
      m.execute();
      m = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
    }
  }

}
