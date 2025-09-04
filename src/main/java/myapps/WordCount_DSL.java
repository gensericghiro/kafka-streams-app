package myapps;

import java.lang.System;
import java.io.*;
import java.time.*;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class WordCount_DSL {

  private static volatile boolean running = true;

  public static void main(String[] args) throws Exception {

    String inputTopic = "word_stream_in";
    String outputTopic = "word_count_out";

    Properties props  = new Properties();
    try {
      props = SimpleProducerConsumer.readConfig("client.properties");
    } catch (IOException e) {
      e.printStackTrace();
    }

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> source = builder.stream(inputTopic);
    source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
        .groupBy((key, value) -> value)
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
        .toStream()
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    final Topology topology = builder.build();
    final KafkaStreams streams = new KafkaStreams(topology, props);
    final CountDownLatch latch = new CountDownLatch(1);

    startProduceCronjob(inputTopic, props);
    startConsumeCronjob(outputTopic, props);


    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }

  public static void startProduceCronjob(String topic, Properties config) {
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    Producer<String, String> producer = new KafkaProducer<>(config);

    Thread produceCronjob = new Thread(() -> {
      while (running) {
        System.out.println("Producing record...");
        try {
          String key = "a_key";
          String value = "a_value";

          producer.send(new ProducerRecord<>(topic, key, value));
          Thread.sleep(10000); // Simulate work
        } catch (Exception e) {
          System.out.println("Produce cronjob interrupted.");
          e.printStackTrace();
          Thread.currentThread().interrupt(); // Restore interrupt status
          running = false; // Exit loop on interrupt
        }
      }
      System.out.println("Produce cronjob gracefully stopped.");
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutdown hook activated: Ctrl+C detected.");
      running = false; // Signal the worker thread to stop
      try {
        producer.close();
        produceCronjob.join(); // Wait for the worker thread to finish its current iteration
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      System.out.println("Produce cronjob shutdown complete.");
    }));

    produceCronjob.start();
    System.out.println("Produce cronjob running. Press Ctrl+C to stop.");
  }

  public static void startConsumeCronjob(String topic, Properties config) {
    // sets the group ID, offset and message deserializers
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "java-group-1");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

    // creates a new consumer instance and subscribes to messages from the topic
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Arrays.asList(topic));

    Thread consumerCronjob = new Thread(() -> {
      while (running) {
        try {
          // polls the consumer for new messages and prints them
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
          for (ConsumerRecord<String, String> record : records) {
            System.out.println(
                String.format(
                    "Consumed message from topic %s: key = %s value = %s", topic, record.key(), record.value()));
          }
        } catch (Exception e) {
          System.out.println("Consume cronjob interrupted.");
          e.printStackTrace();
          Thread.currentThread().interrupt(); // Restore interrupt status
          running = false; // Exit loop on interrupt
        }
      }
      System.out.println("Consume cronjob gracefully stopped.");
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutdown hook activated: Ctrl+C detected.");
      running = false; // Signal the worker thread to stop
      try {
        consumer.close();
        consumerCronjob.join(); // Wait for the worker thread to finish its current iteration
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      System.out.println("Consume cronjob shutdown complete.");
    }));

    consumerCronjob.start();
    System.out.println("Consume cronjob running. Press Ctrl+C to stop.");
  }
}
