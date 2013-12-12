package io.teknek.cassandra;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import junit.framework.Assert;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.ByteSerializer;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.util.MapBuilder;

public class CassandraOperatorTest extends EmbeddedCassandraServer {
  
  @SuppressWarnings("unchecked")
  @Test
  public void testOperator() throws CharacterCodingException, ConnectionException{
    Operator o = new CassandraOperator();
    o.setProperties(MapBuilder.makeMap(CassandraOperator.KEYSPACE, EmbeddedCassandraServer.KEYSPACE,
            CassandraOperator.COLUMN_FAMILY, EmbeddedCassandraServer.COLUMNFAMILY,
            CassandraOperator.HOST_LIST, "localhost:9157",
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
    
    
    ColumnFamily<ByteBuffer, ByteBuffer> cf = ColumnFamily
            .newColumnFamily(COLUMNFAMILY, ByteBufferSerializer.get(),
                    ByteBufferSerializer.get());
    Column<ByteBuffer> result = keyspace.prepareQuery(cf)
            .getKey(ByteBufferUtil.bytes("user1"))
            .getColumn(ByteBufferUtil.bytes("firstname"))
            .execute().getResult();
    Assert.assertEquals("bob", result.getStringValue());
    

    /*
    Keyspace keyspace = HFactory.createKeyspace((String) KEYSPACE, cluster);   
    ColumnQuery<ByteBuffer,ByteBuffer,ByteBuffer> query = HFactory.createColumnQuery(keyspace, ByteBufferSerializer.get(), ByteBufferSerializer.get(), ByteBufferSerializer.get());
    query.setKey(ByteBufferUtil.bytes("user1"));
    query.setName(ByteBufferUtil.bytes("firstname"));
    query.setColumnFamily(COLUMNFAMILY);
    QueryResult<HColumn<ByteBuffer, ByteBuffer>> x = query.execute();
    Assert.assertEquals("bob", ByteBufferUtil.string(x.get().getValue()));
    */
  }
  
}
