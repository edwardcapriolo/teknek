package io.teknek.kafka;

import io.teknek.feed.FeedPartition;
import io.teknek.model.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class TestSimpleKafkaFeed extends EmbeddedKafkaServer {

  private String TOPIC = "simplekafkafeed";
  
  private void sendData() {
    KafkaUtil.createTopic(TOPIC, 1, 1, zookeeperTestServer.getConnectString());
    Map<String, Object> sendProps = new HashMap<String, Object>();
    sendProps.put(KafkaOutputOperator.TOPIC, TOPIC);
    sendProps.put(KafkaOutputOperator.ZOOKEEPER_CONNECT, zookeeperTestServer.getConnectString());
    sendProps.put(KafkaOutputOperator.METADATA_BROKER_LIST, "localhost:9092");
    KafkaOutputOperator o = new KafkaOutputOperator();
    o.setProperties(sendProps);
    for (int i = 0; i < 5; i++) {
      Tuple tout = new Tuple();
      tout.setField(KafkaOutputOperator.KEY_FIELD, "1".getBytes());
      tout.setField(KafkaOutputOperator.MESSAGE_FIELD, (i + "").getBytes());
      o.handleTuple(tout);
    }
  }
  private void receiveData(){
    Map<String,Object> props = new HashMap<String,Object>();
    props.put(SimpleKafkaFeed.CONSUMER_GROUP, "group1");
    props.put(SimpleKafkaFeed.PARTITIONS, 1);
    props.put(SimpleKafkaFeed.TOPIC, TOPIC);
    props.put(SimpleKafkaFeed.ZOOKEEPER_CONNECT, zookeeperTestServer.getConnectString());
    props.put(SimpleKafkaFeed.RESET_OFFSET, "xxx");
    SimpleKafkaFeed sf = new SimpleKafkaFeed(props);
    List<FeedPartition> parts = sf.getFeedPartitions();
    Assert.assertEquals(1, parts.size());
    FeedPartition a = parts.get(0);
    a.initialize();
    Tuple t = new Tuple();
    boolean hasMore = false;
    hasMore = a.next(t);
    Assert.assertTrue(hasMore);
    Assert.assertEquals("0", new String((byte[]) t.getField(SimpleKafkaFeed.MESSAGE_FIELD)));
    hasMore = a.next(t);
    Assert.assertTrue(hasMore);
    Assert.assertEquals("1", new String((byte[]) t.getField(SimpleKafkaFeed.MESSAGE_FIELD)));
    hasMore = a.next(t);
    Assert.assertTrue(hasMore);
    Assert.assertEquals("1", new String((byte[]) t.getField(SimpleKafkaFeed.KEY_FIELD)));
  }
  @Test
  public void aTest(){
    sendData();
    receiveData();
    
    /*
    KafkaUtil.createTopic(TOPIC, 1, 1, zookeeperTestServer.getConnectString());
    Producer<String, String> producer = new Producer<String, String>(super.createProducerConfig());
    for (int i = 0; i < 5; i++) {
      producer.send(new KeyedMessage<String, String>(TOPIC, "1", i + ""));
    }*/
    
  }
}
