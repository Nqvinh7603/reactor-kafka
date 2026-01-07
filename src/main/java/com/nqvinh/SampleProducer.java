/*******************************************************************************
 * Class        ：SampleProducer
 * Created date ：2026/01/07
 * Lasted date  ：2026/01/07
 * Author       ：VinhNQ2
 * Change log   ：2026/01/07：01-00 VinhNQ2 create a new
 ******************************************************************************/
package com.nqvinh;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * SampleProducer
 *
 * @author VinhNQ2
 * @version 01-00
 * @since 01-00
 */
public class SampleProducer {
  private static final Logger log = Loggers.getLogger(SampleProducer.class.getName());

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC = "demo-topic";

  private final KafkaSender<Integer, String> sender;
  private final DateTimeFormatter dateFormat;

  public SampleProducer(String bootstrapServers) {

    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);

    sender = KafkaSender.create(senderOptions);
    dateFormat =
        DateTimeFormatter.ofPattern("HH:mm:ss:SSS z dd MMM yyyy").withZone(ZoneId.systemDefault());
  }

  public void sendMessages(String topic, int count, CountDownLatch latch)
      throws InterruptedException {
    sender
        .<Integer>send(
            Flux.range(1, count)
                .map(i -> SenderRecord.create(new ProducerRecord<>(topic, i, "Message_" + i), i)))
        .doOnError(e -> log.error("Send failed", e))
        .subscribe(
            r -> {
              RecordMetadata metadata = r.recordMetadata();
              Instant timestamp = Instant.ofEpochMilli(metadata.timestamp());
              log.info(
                  "Message {} sent successfully, topic-partition={}-{} offset={} timestamp={}",
                  r.correlationMetadata(),
                  metadata.topic(),
                  metadata.partition(),
                  metadata.offset(),
                  dateFormat.format(timestamp));
              latch.countDown();
            });
  }

  public void close() {
    sender.close();
  }

  public static void main(String[] args) throws Exception {
    int count = 20;
    CountDownLatch latch = new CountDownLatch(count);
    SampleProducer producer = new SampleProducer(BOOTSTRAP_SERVERS);
    producer.sendMessages(TOPIC, count, latch);
    latch.await(10, TimeUnit.SECONDS);
    producer.close();
  }
}
