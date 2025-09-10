package myapps;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
KStreams doesn't let you explicitly set the group protocol, so the only way to change between
classic and consumer is to change the underlying kafka streams version used
 */
public class RebalanceExperiments_KStreams {
  private static final String inputTopic = "orgs";
  private static final String outputTopic = "pkcs_to_orgs";

  private static final Logger LOGGER = LogManager.getLogger();

  private static final Serde<List<String>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.String());

  public static void main(String[] args) {
    Properties props  = new Properties();
    try {
      props = SimpleProducerConsumer.readConfig("client.properties");
    } catch (IOException e) {
      e.printStackTrace();
    }

    myapps.PkcsToOrgs_DSL.startProduceCronjob(inputTopic, props);
    myapps.PkcsToOrgs_DSL.startConsumeCronjob(outputTopic, props);

    KafkaStreams stream1 = newKStreamJob(inputTopic, outputTopic, props);
    KafkaStreams stream2 = newKStreamJob(inputTopic, outputTopic, props);

    stream1.cleanUp();
    stream2.cleanUp();
    final CountDownLatch latch = new CountDownLatch(1);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        stream1.close();
        stream2.close();
        latch.countDown();
      }
    });

    try {
      LOGGER.warn("Starting stream 1");
      stream1.start();
    } catch (Throwable e) {
      System.exit(1);
    }

    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Start a new consumer to see the effect of rebalance
    try {
      LOGGER.warn("Starting stream 2");
      stream2.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }

    System.exit(0);
  }

  public static KafkaStreams newKStreamJob(String inputTopic, String outputTopic, Properties props) {
    // Generate a random UUID for the state directory
    UUID randomUuid = UUID.randomUUID();
    String uuidString = randomUuid.toString();

    props.put(StreamsConfig.STATE_DIR_CONFIG, "/var/folders/x_/5bhm0y0135xb3p1hp62kcfbr0000gp/T/kafka-streams/rebalance-experiments-kstreams-" + uuidString);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rebalance-experiments-kstreams");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> source = builder.stream(inputTopic);
    source
        .map((key, value) -> KeyValue.pair(value, key))
        .groupByKey()
        .aggregate(
            ArrayList::new,
            (pkcId, orgId, orgList) -> {
              System.out.printf("orgId = %s, pkcId = %s%n", orgId, pkcId);
              if (!orgList.contains(orgId)) {
                orgList.add(orgId);
              }
              return orgList;
            },
            Materialized.<String, List<String>, KeyValueStore<Bytes, byte[]>>as(
                    "rebalance-experiments-kstreams-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(listSerde))
        .toStream()
        .mapValues(Object::toString)
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


    final Topology topology = builder.build();

    return new KafkaStreams(topology, props);
  }
}
