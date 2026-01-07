/*******************************************************************************
 * Class        ：SampleConsumer
 * Created date ：2026/01/07
 * Lasted date  ：2026/01/07
 * Author       ：VinhNQ2
 * Change log   ：2026/01/07：01-00 VinhNQ2 create a new
 ******************************************************************************/
package com.nqvinh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * SampleConsumer
 *
 * @version 01-00
 * @since 01-00
 * @author VinhNQ2
 */
public class SampleConsumer {

  private static final Logger log = Loggers.getLogger(SampleProducer.class.getName());

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC = "demo-topic";

  private final ReceiverOptions<Integer, String> receiverOptions;
  private final DateTimeFormatter dateFormat;

  /**
   * Instantiates a new Sample consumer.
   *
   * @param bootstrapServers the bootstrap servers
   */
  public SampleConsumer(String bootstrapServers) {

    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    receiverOptions = ReceiverOptions.create(props);
    dateFormat =
        DateTimeFormatter.ofPattern("HH:mm:ss:SSS z dd MMM yyyy").withZone(ZoneId.systemDefault());
  }

  /**
   * Consume messages disposable.
   *
   * @param topic the topic
   * @param latch the latch
   * @return the disposable
   */
  public Disposable consumeMessages(String topic, CountDownLatch latch) {

    ReceiverOptions<Integer, String> options =
        receiverOptions
            .subscription(Collections.singleton(topic))
            .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
            .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
    Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive();
    return kafkaFlux.subscribe(
        record -> {
          ReceiverOffset offset = record.receiverOffset();
          Instant timestamp = Instant.ofEpochMilli(record.timestamp());
          log.info(
              "Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
              offset.topicPartition(),
              offset.offset(),
              dateFormat.format(timestamp),
              record.key(),
              record.value());
          offset.acknowledge();
          latch.countDown();
        });
  }

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    int count = 20;
    CountDownLatch latch = new CountDownLatch(count);
    SampleConsumer consumer = new SampleConsumer(BOOTSTRAP_SERVERS);
    Disposable disposable = consumer.consumeMessages(TOPIC, latch);
    latch.await(10, TimeUnit.SECONDS);
    disposable.dispose();
  }
}
