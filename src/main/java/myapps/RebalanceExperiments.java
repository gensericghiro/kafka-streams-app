package myapps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RebalanceExperiments {
  private static volatile boolean running = true;
  private static final String topic = "rebalance_experiments";
  private static final String group_protocol = "consumer";
//  private static final String group_protocol = "classic";
  private static final Logger LOGGER = LogManager.getLogger();

  public static void main(String[] args) {
    LOGGER.warn("This is a test");
    Properties props  = new Properties();
    try {
      props = SimpleProducerConsumer.readConfig("client.properties");
    } catch (IOException e) {
      e.printStackTrace();
    }

    final CountDownLatch latch = new CountDownLatch(1);
    startProduceCronjob(topic, props);
    startConsumeCronjob(topic, props);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // Start a new consumer to see the effect of rebalance
    LOGGER.warn("Starting new consumer to see the effect of a rebalance");
    startConsumeCronjob(topic, props);


    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        latch.countDown();
      }
    });

    try {
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
          Thread.sleep(2000); // Simulate work
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
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, group_protocol);

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
