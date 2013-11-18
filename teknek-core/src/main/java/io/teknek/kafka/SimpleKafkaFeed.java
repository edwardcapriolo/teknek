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
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;


import io.teknek.feed.Feed;
import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;

/**
 * This feed consumes events from kafka topics using the high level client. This client
 * will automatically handle rebalancing and offset storage. If you want to bind consumers
 * to specific partitions predictable do not use this.
 * @author edward
 *
 */
public class SimpleKafkaFeed extends Feed {
  
  public static final String TOPIC = "simple.kafka.feed.topic";
  public static final String CONSUMER_GROUP = "simple.kafka.feed.consumer.group";
  public static final String RESET_OFFSET = "simple.kafka.feed.reset.offset";
  public static final String ZOOKEEPER_CONNECT = "simple.kafka.feed.zookeeper.connect";
  public static final String STREAMS_PER_WORKER = "simple.kafka.feed.streams.per.worker";
  public static final String PARTITIONS = "simple.kafka.feed.partitions";
  
  public SimpleKafkaFeed(Map<String, Object> properties) {
    super(properties);
  }

  @Override
  public List<FeedPartition> getFeedPartitions() {
    List<FeedPartition> results = new ArrayList<FeedPartition>();
    Integer parts = (Integer) properties.get(PARTITIONS);
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

  ConsumerIterator<byte[], byte[]> iterator;
  
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
    ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    Map<String, Integer> consumers = new HashMap<String, Integer>();
    if (feed.getProperties().get(SimpleKafkaFeed.STREAMS_PER_WORKER) == null){
      consumers.put(feed.getProperties().get(SimpleKafkaFeed.TOPIC).toString(), 1);
    } else {
      consumers.put(feed.getProperties().get(SimpleKafkaFeed.TOPIC).toString(), 
              (Integer)(feed.getProperties().get(SimpleKafkaFeed.STREAMS_PER_WORKER)));
    }
    Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = consumerConnector
            .createMessageStreams(consumers);
    final List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get(feed.getProperties().get(SimpleKafkaFeed.TOPIC).toString());
    final KafkaStream<byte[], byte[]> stream = streams.get(0);
    iterator = stream.iterator();
  }

  @Override
  public boolean next(ITuple t) {
    MessageAndMetadata<byte[],byte[]> message = iterator.next();
    t.setField("message", message.message());
    t.setField("key", message.key());
    return false;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub 
  }
  
}

/*
StringDecoder decoder = new StringDecoder(new VerifiableProperties());
Map<String, List<KafkaStream<String, String>>> topicMessageStreams = consumerConnector
       .createMessageStreams(consumers, decoder, decoder);
final List<KafkaStream<String, String>> streams = topicMessageStreams.get(feed.getProperties().get(SimpleKafkaFeed.TOPIC).toString());
*/