package io.teknek.cassandra;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map; 

import junit.framework.Assert;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.util.MapBuilder;

public class CassandraOperatorTest extends EmbeddedCassandraServer {

  @SuppressWarnings("unchecked")
  @Test
  public void testOperator() throws CharacterCodingException{
    Operator o = new CassandraOperator();
    o.setProperties(MapBuilder.makeMap(CassandraOperator.KEYSPACE, EmbeddedCassandraServer.KEYSPACE,
            CassandraOperator.COLUMN_FAMILY, EmbeddedCassandraServer.COLUMNFAMILY,
            CassandraOperator.HOST_LIST, "localhost:9157"));
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
    
    Keyspace keyspace = HFactory.createKeyspace((String) KEYSPACE, cluster);   
    ColumnQuery<ByteBuffer,ByteBuffer,ByteBuffer> query = HFactory.createColumnQuery(keyspace, ByteBufferSerializer.get(), ByteBufferSerializer.get(), ByteBufferSerializer.get());
    query.setKey(ByteBufferUtil.bytes("user1"));
    query.setName(ByteBufferUtil.bytes("firstname"));
    query.setColumnFamily(COLUMNFAMILY);
    QueryResult<HColumn<ByteBuffer, ByteBuffer>> x = query.execute();
    Assert.assertEquals("bob", ByteBufferUtil.string(x.get().getValue()));
  }
  
}
