package io.teknek.kafka;

import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;

public class KafkaOutputOperator extends Operator {

  public static final String TOPIC = "kafka.output.topic";
  public static final String ZOOKEEPER_CONNECT = "kafka.output.zookeeper.connect";
  public static final String METADATA_BROKER_LIST = "kafka.output.metadata.broker.list";
  public static final String KEY_FIELD = "kafka.output.key.field";
  public static final String MESSAGE_FIELD = "kafka.output.message.field";
  
  Producer<byte [], byte[]> producer = null;
  
  @Override
  public void setProperties(Map<String, Object> properties) {
    super.setProperties(properties);
    Properties producerProps = new Properties();
    producerProps.put("zookeeper.connect", (String) properties.get(ZOOKEEPER_CONNECT));
    producerProps.setProperty("batch.size", "1");
    producerProps.setProperty("producer.type", "async");
    producerProps.put("metadata.broker.list", (String) properties.get(METADATA_BROKER_LIST));
    ProducerConfig producerConfig = new ProducerConfig(producerProps);
    producer = new Producer<byte[], byte []>(producerConfig);
  }

  @Override
  public void handleTuple(ITuple t) {
    producer.send(new KeyedMessage<byte[], byte[]>((String) properties.get(TOPIC),
            (byte[]) t.getField(KEY_FIELD),
            (byte[]) t.getField(MESSAGE_FIELD)));   
  }

}
