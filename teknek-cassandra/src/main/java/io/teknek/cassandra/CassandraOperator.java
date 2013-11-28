package io.teknek.cassandra;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.testutils.EmbeddedServerHelper;


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
  
  protected CassandraHostConfigurator cassandraHostConfigurator;
  protected String clusterName = "TestCluster";
  protected Cluster cluster;
  protected Keyspace keyspace;
  
  @Override
  public void setProperties(Map<String, Object> properties) {
    super.setProperties(properties);   
    cassandraHostConfigurator = new CassandraHostConfigurator(((String) properties.get(HOST_LIST)));
    cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator);
    keyspace = HFactory.createKeyspace((String) properties.get(KEYSPACE), cluster);   
  }
  
  @Override
  public void handleTuple(ITuple tuple) {
    Mutator<ByteBuffer> m = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
    HColumn col = HFactory.createColumn((ByteBuffer) tuple.getField(COLUMN), (ByteBuffer) tuple.getField(VALUE), System.nanoTime(), ByteBufferSerializer.get(), ByteBufferSerializer.get());
    m.addInsertion((ByteBuffer) tuple.getField(ROW_KEY), (String) properties.get(COLUMN_FAMILY), col);
    MutationResult result = m.execute();
  }

}