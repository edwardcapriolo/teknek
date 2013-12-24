package io.teknek.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;


import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;

/**
 * This feed consumes events from kafka topics using the high level client. This client
 * will automatically handle rebalancing and offset storage. 
 * This feed produces tuples with two columns key and message both of type byte [].
 * @author edward
 *
 */
public class SimpleKafkaFeed extends Feed {
  
  public static final String KEY_FIELD = "key";
  public static final String MESSAGE_FIELD = "message";
  public static final String TOPIC = "simple.kafka.feed.topic";
  public static final String CONSUMER_GROUP = "simple.kafka.feed.consumer.group";
  public static final String RESET_OFFSET = "simple.kafka.feed.reset.offset";
  public static final String ZOOKEEPER_CONNECT = "simple.kafka.feed.zookeeper.connect";
  //public static final String STREAMS_PER_WORKER = "simple.kafka.feed.streams.per.worker";
  public static final String PARTITIONS = "simple.kafka.feed.partitions";
  
  public SimpleKafkaFeed(Map<String, Object> properties) {
    super(properties);
  }

  @Override
  public List<FeedPartition> getFeedPartitions() {
    List<FeedPartition> results = new ArrayList<FeedPartition>();
    
    Integer parts = ((Number) properties.get(PARTITIONS)).intValue();
    for (int i = 0; i < parts; i++) {
      SimpleKafkaFeedPartition skf = new SimpleKafkaFeedPartition(this, i+"");
      results.add(skf);
    }
    return results;
  }

  @Override
  public Map<String, String> getSuggestedBindParams() {
    return new HashMap<String, String>();
  }
  
}

class SimpleKafkaFeedPartition extends FeedPartition {

  protected ConsumerIterator<byte[], byte[]> iterator;
  protected ConsumerConnector consumerConnector;
  
  public SimpleKafkaFeedPartition(Feed feed, String partitionId) {
    super(feed, partitionId);
  }

  @Override
  public void initialize() {
    Properties consumerProperties = new Properties();
    consumerProperties.put("zookeeper.connect", feed.getProperties().get(SimpleKafkaFeed.ZOOKEEPER_CONNECT));
    consumerProperties.put("group.id", feed.getProperties().get(SimpleKafkaFeed.CONSUMER_GROUP));
    if (feed.getProperties().get(SimpleKafkaFeed.RESET_OFFSET) != null){
      consumerProperties.put("auto.offset.reset", "smallest");
    }
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProperties);
    consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    
    Map<String, Integer> consumers = new HashMap<String, Integer>();
    consumers.put(feed.getProperties().get(SimpleKafkaFeed.TOPIC).toString(), 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = consumerConnector
            .createMessageStreams(consumers);
    final List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get(feed.getProperties().get(SimpleKafkaFeed.TOPIC).toString());
    final KafkaStream<byte[], byte[]> stream = streams.get(0);
    iterator = stream.iterator();
    
  }

  @Override
  public boolean next(ITuple t) {
    MessageAndMetadata<byte[],byte[]> message = iterator.next();
    t.setField(SimpleKafkaFeed.MESSAGE_FIELD, message.message());
    t.setField(SimpleKafkaFeed.KEY_FIELD, message.key());
    return true;
  }

  @Override
  public void close() {
    consumerConnector.shutdown();
  }

  @Override
  public String getOffset() {
    throw new UnsupportedOperationException("This feed does not support offsets");
  }

  @Override
  public boolean supportsOffsetManagement() {
    return false;
  }

  @Override
  public void setOffset(String offset) {
    throw new UnsupportedOperationException("This feed does not support offsets");
  }
  
}