package s.j.liu.kafkautils;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

  /**
   * kafka-topics.sh --create --zookeeper 172.16.42.5:2181 --replication-factor
   * 2 --partitions 12 --topic car </br>
   * kafka-topics.sh --delete --zookeeper 172.16.42.5:2181 --replication-factor
   * 2 --partitions 12 --topic car </br>
   */

  /**
   * 测试.
   * 
   * @param args
   *          Formal Parameter
   */
  public static void main(String[] args) {
    boolean s = true;
    if (s) {
      KafkaProducerUtils u = new KafkaProducerUtils("192.168.56.2:9092,192.168.56.3:9092,192.168.56.4:9092");
      List<String> ls = new ArrayList<String>();
      long l = System.currentTimeMillis();
      for (int i = 0; i < 1000; i++) {
        ls.add(String.valueOf(i));
      }
      u.sendListMessage("test", ls);
      long m = System.currentTimeMillis();
      System.out.println(m - l); // 551
    } else {
      KafkaProducerUtils u = new KafkaProducerUtils("192.168.56.2:9092,192.168.56.3:9092,192.168.56.4:9092");
      long l = System.currentTimeMillis();
      for (int i = 0; i < 10000; i++) {
        u.sendOneMessage("test", String.valueOf(i));
      }
      long m = System.currentTimeMillis();
      System.out.println(m - l); // 55586
    }
  }

  /**
   * Create the test case.
   *
   * @param testName
   *          name of the test case
   */
  public AppTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(AppTest.class);
  }

  /**
   * Rigourous Test :-)
   */
  public void testApp() {
    assertTrue(true);
  }
}
