package s.j.liu.kafkautils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @version v0.0.1
 * @since 2017-06-29 09:29:00
 * @author Shengjun Liu
 *
 */
public class KafkaProducerUtils {
  private Producer<String, String> producer = null;
  private Properties props = new Properties();
  private List<ProducerRecord<String, String>> kafkaData = null;
  private long localCacheSize = 100L;

  /**
   * Construction Method.
   * 
   * @param kafkaIp
   *          gpu1:9092,gpu2:9092
   */
  public KafkaProducerUtils(String kafkaIp) {
    props.put("bootstrap.servers", kafkaIp);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 33554432);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 335544320);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producer = new KafkaProducer<String, String>(props);
    kafkaData = new ArrayList<ProducerRecord<String, String>>();
  }

  /**
   * set Local Cache Size.
   * 
   * @param size
   *          Local Cache Size
   * @return this
   */
  public KafkaProducerUtils setLocalCacheSize(long size) {
    this.localCacheSize = size;
    return this;
  }

  /**
   * Send One Message to Local Cache.
   * 
   * @param topic
   *          topic name
   * @param msg
   *          message contents
   */
  public void sendOneMessageUseLocalCache(String topic, String msg) {
    synchronized (kafkaData) {
      kafkaData.add(new ProducerRecord<String, String>(topic, msg));
      if (localCacheSize < kafkaData.size()) {
        sendLocalCache();
      }
    }
  }

  /**
   * Send a set number of Messages to Local Cache.
   * 
   * @param topic
   *          topic name
   * @param msgs
   *          message contents
   */
  public void sendListMessageUseLocalCache(String topic, List<String> msgs) {
    msgs.forEach(msg -> {
      sendOneMessageUseLocalCache(topic, msg);
    });
  }

  /**
   * Send a set number of Local Cache to Kafka Broker.
   */
  public void sendLocalCache() {
    synchronized (kafkaData) {
      kafkaData.forEach(msg -> {
        producer.send(msg);
      });
      producer.flush();
      kafkaData.clear();
    }
  }

  /**
   * Send One Message to Kafka Broker.
   * 
   * @param topic
   *          topic name
   * @param msg
   *          message contents
   */
  public void sendOneMessage(String topic, String msg) {
    producer.send(new ProducerRecord<String, String>(topic, msg));
    producer.flush();
  }

  /**
   * Send a set number of Messages to Kafka Broker.
   * 
   * @param topic
   *          topic name
   * @param msgs
   *          message contents
   */
  public void sendListMessage(String topic, List<String> msgs) {
    msgs.forEach(msg -> {
      producer.send(new ProducerRecord<String, String>(topic, msg));
    });
    producer.flush();
  }

  /**
   * Flush Kafka Producer.
   */
  public void flushKafkaProducer() {
    producer.flush();
  }

  /**
   * Close Kafka Producer.
   */
  public void closeKafkaProducer() {
    producer.flush();
    producer.close();
  }
}
