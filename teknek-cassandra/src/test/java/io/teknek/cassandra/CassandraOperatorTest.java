package io.teknek.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;
import io.teknek.util.MapBuilder;

public class CassandraOperatorTest extends EmbeddedCassandraServer {

  @SuppressWarnings("unchecked")
  @Test
  public void testOperator(){
    Operator o = new CassandraOperator();
    o.setProperties(MapBuilder.makeMap(CassandraOperator.KEYSPACE, EmbeddedCassandraServer.KEYSPACE,
            CassandraOperator.COLUMN_FAMILY, EmbeddedCassandraServer.COLUMNFAMILY,
            CassandraOperator.HOST_LIST, "localhost:9157"));
    ITuple t = new Tuple()
      .withField(CassandraOperator.ROW_KEY, ByteBufferUtil.bytes("user1"))
      .withField(CassandraOperator.COLUMN, ByteBufferUtil.bytes("firstname"))
      .withField(CassandraOperator.VALUE, ByteBufferUtil.bytes("bob"));
    o.handleTuple(t);
  }
  
}
