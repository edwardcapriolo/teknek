package io.teknek.kafka;

import io.teknek.datalayer.KafkaUtil;
import io.teknek.feed.FeedPartition;
import io.teknek.model.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.junit.Test;

public class TestSimpleKafkaFeed extends EmbeddedKafkaServer {

  private String TOPIC = "simplekafkafeed";
  @Test
  public void aTest(){
    KafkaUtil.createTopic(TOPIC, 1, 1, zookeeperTestServer.getConnectString());
    Producer<String, String> producer = new Producer<String, String>(super.createProducerConfig());
    for (int i = 0; i < 5; i++) {
      producer.send(new KeyedMessage<String, String>(TOPIC, "1", i + ""));
    }
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
}
