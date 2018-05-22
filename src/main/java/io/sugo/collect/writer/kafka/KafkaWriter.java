package io.sugo.collect.writer.kafka;

import com.google.gson.Gson;
import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.kafka.clients.producer.*;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;


/**
 * Created by fengxj on 4/8/17.
 */
public class KafkaWriter extends AbstractWriter {
  private final Logger logger = LoggerFactory.getLogger(KafkaWriter.class);
  private final Producer<Integer, String> producer;
  private static final String KAFKA_CONFIG_PREFIX = "kafka.";
  private static final String KAFKA_TOPIC = "writer.kafka.topic";
  private static final String KAFKA_ZOOKEEPER_SERVER = "writer.kafka.zookeeper.server";
  private static final String KAFKA_ZOOKEEPER_TIMEOUT = "writer.kafka.zookeeper.timeout";
  private static final String KAFKA_ZOOKEEPER_HOSTS_PATH = "writer.kafka.zookeeper.hosts.path";
  private String topic;
  private String zookeeper;
  private int zkTimeout;
  private String zkHostsPath;
  private String brokerList;
  private int currentRetryTime = 0;

  public KafkaWriter(Configure conf) {
    super(conf);
    this.topic = conf.getProperty(KAFKA_TOPIC);
    Properties properties = conf.getProperties();
    Properties newProperties = new Properties();
    for (Object key : properties.keySet()) {
      String keyStr = key.toString();
      if (keyStr.startsWith(KAFKA_CONFIG_PREFIX)) {
        newProperties.put(keyStr.substring(KAFKA_CONFIG_PREFIX.length()), properties.getProperty(keyStr));
      }
    }
    this.zookeeper = conf.getProperty(KAFKA_ZOOKEEPER_SERVER);
    if (zookeeper !=null && !zookeeper.isEmpty()) {
      this.zkTimeout = Integer.parseInt(conf.getProperty(KAFKA_ZOOKEEPER_TIMEOUT, "10000"));
      this.zkHostsPath = conf.getProperty(KAFKA_ZOOKEEPER_HOSTS_PATH);
      this.brokerList = getClusterViz(zookeeper);
      newProperties.setProperty("bootstrap.servers", brokerList);
    }
    producer = new KafkaProducer<>(newProperties);
  }

  private String getClusterViz(String zookeeper) {

    ZooKeeper zkClient = null;
    StringBuilder hostBuilder = new StringBuilder("");
    try {
      zkClient = new ZooKeeper(zookeeper, this.zkTimeout, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
        }
      });
      Gson gson = new Gson();
      List<String> ids = zkClient.getChildren(this.zkHostsPath, false);
      for (String id : ids) {
        String brokerInfo = new String(zkClient.getData(this.zkHostsPath + "/" + id, false, null));
        Map hostMap = gson.fromJson(brokerInfo, Map.class);
        hostBuilder.append(hostMap.get("host"));
        hostBuilder.append(":");
        hostBuilder.append(hostMap.get("port"));
        hostBuilder.append(",");
      }
    } catch (Exception ex) {
      logger.error("", ex);
    } finally {
      // 关闭zookeeper连接
      if (null != zkClient) {
        try {
          zkClient.close();
        } catch (InterruptedException e) {
          logger.error("", e);
        }
      }
      String hostString = hostBuilder.toString();
      if (0 < hostString.length()) {
        return hostString.substring(0, hostString.length() - 1);
      } else {
        return "";
      }
    }
  }

  @Override
  public boolean write(List<String> messages) {
    long now = 0;
//    if (logger.isDebugEnabled()) {
//      now = System.currentTimeMillis();
//      logger.debug("send to kafka, message size:" + messages.size());
//    }

    Map<Future<RecordMetadata>, String> messageFutureMap = new HashMap<>();
    for (String message : messages) {
      Future<RecordMetadata> future = producer.send(new ProducerRecord<Integer, String>(this.topic, message));
      messageFutureMap.put(future, message);
    }
    producer.flush();

    List<String> errMsgList = null;
    for (Future<RecordMetadata> future : messageFutureMap.keySet()) {
      try {
        future.get();
      } catch (Exception e) {
        logger.error("", e);
        String message = messageFutureMap.get(future);
        if (errMsgList == null)
          errMsgList = new ArrayList<>();

        errMsgList.add(message);
      }
    }

    if (errMsgList != null && errMsgList.size() > 0) {
      currentRetryTime += 1;
      if (currentRetryTime >= 5) {
        logger.error("failed to send to kafka ,error:" + errMsgList.size());
        currentRetryTime = 0;
        return false;
      }
      logger.warn("failed to send to kafka, error:" + errMsgList.size() + " retrying...");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return write(errMsgList);
    }

//    if (logger.isDebugEnabled()) {
//      long current = System.currentTimeMillis();
//      logger.debug("success to send to kafka, druration(ms):" + (current - now));
//    }
    currentRetryTime = 0;
    return true;
  }

  public void setTopic(String topic){
    this.topic = topic;
  }
}
