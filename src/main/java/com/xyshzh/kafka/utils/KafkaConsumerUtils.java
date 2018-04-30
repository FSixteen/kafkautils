package com.xyshzh.kafka.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @version v0.0.1
 * @since 2017-06-29 09:29:00
 * @author Shengjun Liu
 *
 */
public class KafkaConsumerUtils {
  private KafkaConsumer<String, String> consumer = null;
  private Properties props = new Properties();
  private ArrayList<String> kafkaData = new ArrayList<String>();
  private List<String> topics = null;
  private CountDownLatch cdl = new CountDownLatch(1);
  private boolean status = true;
  private long cacheSize = 1000L;
  private long pollSize = 1000L;

  /**
   * Construction Method.
   * 
   * @param kafkaIp
   *          gpu1:9092,gpu2:9092
   * @param groupId
   *          testGroupId
   * @param zkHosts
   *          gpu1:2181,gpu2:2181/kafka
   * @param offset
   *          auto.offset.reset
   * @param topics
   * 
   * 
   */
  public KafkaConsumerUtils(String kafkaIp, String groupId, String zkHosts, List<String> topics, String offset) {
    this.topics = topics;
    props.put("bootstrap.servers", kafkaIp);
    props.put("enable.auto.commit", "true");
    props.put("group.id", groupId);
    if (null != zkHosts) {
      props.put("zookeeper.connect", zkHosts);
    }
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    if (null != offset) {
      props.put("auto.offset.reset", offset);
    }
    consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(topics);
  }

  /**
   * Reset Kafka Consumer.
   */
  public void resetConsumer() {
    consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(topics);
  }

  /**
   * Close Kafka Consumer.
   */
  public void closeKafkaConsumer() {
    try {
      status = false;
      cdl.await();
      consumer.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Set cache size.
   * 
   * @param cacheSize
   *          Cache Size
   * @return
   */
  public KafkaConsumerUtils setCacheSize(long cacheSize) {
    this.cacheSize = cacheSize;
    return this;
  }

  /**
   * Set Poll Size.
   * 
   * @param pollSize
   *          Poll Size
   * @return
   */
  public KafkaConsumerUtils setPollSize(long pollSize) {
    this.pollSize = pollSize;
    return this;
  }

  /**
   * Initialization.
   */
  public void init() {
    new Thread(new Runnable() {
      public void run() {
        while (status) {
          try {
            if (getKafkaDataSize() > cacheSize) {
              try {
                Thread.sleep(500);
                continue;
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            ConsumerRecords<String, String> records = consumer.poll(pollSize);
            for (ConsumerRecord<String, String> record : records) {
              kafkaData.add(record.value());
            }
            System.out.println("kafka local data add " + records.count() + " times......");
            consumer.commitSync();
            records = null;
          } catch (Exception e) {
            try {
              consumer.close();
            } finally {
              resetConsumer();
            }
          }
        }
        cdl.countDown();
        System.out.println("I'm KafkaConsumerUtils's init method,i'm over!");
      }
    }).start();
  }

  /**
   * Get Kafka Data Size.
   * 
   * @return int
   */
  public int getKafkaDataSize() {
    return kafkaData.size();
  }

  /**
   * Clear Kafka Data.
   */
  public void clearKafkaData() {
    kafkaData.clear();
  }

  /**
   * Get one Kafka Data.
   * 
   * @return String
   */
  public String getKafkaData() {
    synchronized (this) {
      if (getKafkaDataSize() > 0) {
        return kafkaData.remove(0);
      } else {
        return null;
      }
    }
  }

  /**
   * Get a set number of Kafka Data.
   * 
   * @param size
   *          List Size
   * 
   * @return List
   */
  public List<String> getKafkaData(int size) {
    List<String> list = new ArrayList<String>();
    for (int i = 0; i < size; i++) {
      String data = getKafkaData();
      if (null != data) {
        list.add(data);
      }
    }
    return list;
  }

  /**
   * Copy one Kafka Data.
   * 
   * @return String
   */
  public String copyKafkaData() {
    if (0 < getKafkaDataSize()) {
      return kafkaData.get(0);
    } else {
      return null;
    }
  }

  /**
   * Get Kafka Consumer.
   * 
   * @return
   */
  public KafkaConsumer<String, String> getConsumer() {
    return this.consumer;
  }

  /**
   * Get Kafka Consumer.
   * 
   * @return
   */
  public KafkaConsumer<String, String> getKafkaConsumer() {
    return this.consumer;
  }
}
