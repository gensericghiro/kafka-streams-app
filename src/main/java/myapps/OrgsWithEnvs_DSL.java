package myapps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class OrgsWithEnvs_DSL {

  private static volatile boolean running = true;

  private static final String inputOrgToEnvTopic = "org_to_env";
  private static final String inputEnvToLkcTopic = "env_to_lkc";

  private static final String outputTopic = "org_to_lkc";

  private static final String[] orgs = {"Org1", "Org2", "Org3"};
  private static final String[] envs = {"Env1", "Env2", "Env3"};
  private static final String[] lkcs = {"Lkc1", "Lkc2", "Lkc3"};
  private static final Random random = new Random(0);

  private static final Serde<List<String>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.String());

  public static void main(String[] args) {

    Properties props  = new Properties();
    try {
      props = SimpleProducerConsumer.readConfig("client.properties");
    } catch (IOException e) {
      e.printStackTrace();
    }

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-org_to_env-dsl");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> orgToEnv = builder.stream(inputOrgToEnvTopic);
    KStream<String, String> envToLkc = builder.stream(inputEnvToLkcTopic);

    KStream<String, String> envToOrg = orgToEnv.map((key, value) -> KeyValue.pair(value, key));

    KStream<String, String> orgToLkc = envToLkc.join(envToOrg,
        (lkc, org) -> lkc + ": " + org,
        JoinWindows.of(Duration.ofMinutes(5)), // JoinWindow
        StreamJoined.with(
            Serdes.String(), // Key Serde
            Serdes.String(), // Left Value Serde
            Serdes.String()  // Right Value Serde
        )
    );

    orgToLkc.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

    final Topology topology = builder.build();
    final KafkaStreams streams = new KafkaStreams(topology, props);
    streams.cleanUp();
    final CountDownLatch latch = new CountDownLatch(1);

    startProduceCronjob(inputOrgToEnvTopic, inputEnvToLkcTopic, props);
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

  public static void startProduceCronjob(String orgToEnvTopic, String envToLkcTopic, Properties config) {
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    Producer<String, String> producer = new KafkaProducer<>(config);

    Thread produceCronjob = new Thread(() -> {
      while (running) {
        String randomOrg = orgs[random.nextInt(orgs.length)];
        String randomEnv = envs[random.nextInt(envs.length)];
        String randomLkc = lkcs[random.nextInt(lkcs.length)];
        System.out.printf("Producing records (org=%s, env=%s) & (env=%s, lkc=%s)...%n",
            randomOrg, randomEnv, randomEnv,  randomLkc);
        try {
          producer.send(new ProducerRecord<>(orgToEnvTopic, randomOrg, randomEnv));
          producer.send(new ProducerRecord<>(envToLkcTopic, randomEnv, randomLkc));
          Thread.sleep(5000); // Simulate work
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
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "orgToLkc-group");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    // creates a new consumer instance and subscribes to messages from the topic
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Arrays.asList(topic));

    Thread consumerCronjob = new Thread(() -> {
      while (running) {
        try {
          // polls the consumer for new messages and prints them
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
          for (ConsumerRecord<String, String> record : records) {
            System.out.printf(
                "Consumed message from topic %s: key = %s value = %s%n", topic, record.key(), record.value());
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
