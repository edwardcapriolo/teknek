package io.teknek.cassandra;
  
import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.util.MapBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import junit.framework.Assert;


import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferSerializer;

public class CassandraBatchingOperatorTest extends EmbeddedCassandraServer { 
  
  @SuppressWarnings("unchecked") 
  @Test
  public void testOperator() throws CharacterCodingException, ConnectionException{
    Operator o = new CassandraBatchingOperator();
    o.setProperties(MapBuilder.makeMap(CassandraOperator.KEYSPACE, EmbeddedCassandraServer.KEYSPACE,
            CassandraOperator.COLUMN_FAMILY, EmbeddedCassandraServer.COLUMNFAMILY,
            CassandraOperator.HOST_LIST, "localhost:9157", 
            CassandraBatchingOperator.BATCH_SIZE, 2,
            CassandraOperator.PORT, 9157));
    ITuple t = new Tuple()
      .withField(CassandraOperator.ROW_KEY, ByteBufferUtil.bytes("user1"))
      .withField(CassandraOperator.COLUMN, ByteBufferUtil.bytes("firstname"))
      .withField(CassandraOperator.VALUE, ByteBufferUtil.bytes("bob"));
    o.handleTuple(t);
    
    ITuple k = new Tuple()
    .withField(CassandraOperator.ROW_KEY, ByteBufferUtil.bytes("user1"))
    .withField(CassandraOperator.COLUMN, ByteBufferUtil.bytes("lastname"))
    .withField(CassandraOperator.VALUE, ByteBufferUtil.bytes("smith"));
    o.handleTuple(k);
    
    ITuple l = new Tuple()
    .withField(CassandraOperator.ROW_KEY, ByteBufferUtil.bytes("user1"))
    .withField(CassandraOperator.COLUMN, ByteBufferUtil.bytes("middlename"))
    .withField(CassandraOperator.VALUE, ByteBufferUtil.bytes("eli"));
    o.handleTuple(l);
    assertResults();
    assertNotThere();
  }
  
  
  public void assertResults() throws CharacterCodingException, ConnectionException {
    ColumnFamily<ByteBuffer, ByteBuffer> cf = ColumnFamily
            .newColumnFamily(COLUMNFAMILY, ByteBufferSerializer.get(),
                    ByteBufferSerializer.get());
    Column<ByteBuffer> result = keyspace.prepareQuery(cf)
            .getKey(ByteBufferUtil.bytes("user1"))
            .getColumn(ByteBufferUtil.bytes("firstname"))
            .execute().getResult();
    Assert.assertEquals("bob", result.getStringValue());
  }

  public void assertNotThere() throws CharacterCodingException, ConnectionException {
    try {
      ColumnFamily<ByteBuffer, ByteBuffer> cf = ColumnFamily.newColumnFamily(COLUMNFAMILY,
              ByteBufferSerializer.get(), ByteBufferSerializer.get());
      keyspace.prepareQuery(cf).getKey(ByteBufferUtil.bytes("user1"))
              .getColumn(ByteBufferUtil.bytes("middlename")).execute().getResult();
      Assert.fail("Should have not found");
    } catch (NotFoundException ex) {
    }
  }

}
