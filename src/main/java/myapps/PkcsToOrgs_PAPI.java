package myapps;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.util.*;

public class PkcsToOrgs_PAPI {
  private static final String inputTopic = "orgs";
  private static final String outputTopic = "pkcs_to_orgs";
  private static final String repartitionTopic = "pkcs_to_orgs_repartition";

  private static final Serde<List<String>> listSerde = Serdes.ListSerde(ArrayList.class, Serdes.String());
  private static final StringSerializer stringSerializer = new StringSerializer();

  public static void main(String[] args) {
    Properties props = new Properties();

    try {
      props = SimpleProducerConsumer.readConfig("client.properties");
    } catch (IOException e) {
      e.printStackTrace();
    }

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pkcs_to_orgs-papi");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    Topology topology = new Topology();

    // 1. Add a source node to read from an input topic
    topology.addSource("Source", inputTopic);

    // 2. Re-key the original event
    topology.addProcessor("DefineNewKey", NewKeyProcessor::new, "Source");

    // 3. publish re-keyed event to a repartition topic
    topology.addSink("Repartition-sink", repartitionTopic, "DefineNewKey");

    // 4. read from repartition topic
    topology.addSource("Repartitioned-source", repartitionTopic);

    // 5. Add a processor node with custom logic and state store access
    topology.addProcessor("Process", PkcsToOrgsProcessor::new, "Repartitioned-source");

    // 6. Define a state store for pkc to orgs aggregations
    topology.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("OrgAggregations"),
        Serdes.String(), listSerde),
        "Process"); // Associate with the "Process" node

    // 7. Add a sink node to write results to an output topic
    topology.addSink("Sink", outputTopic, stringSerializer, stringSerializer, "Process");

    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.cleanUp();

    PkcsToOrgs_DSL.startProduceCronjob(inputTopic, props);
    PkcsToOrgs_DSL.startConsumeCronjob(outputTopic, props);

    try {
      streams.start();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Add shutdown hook to close the Streams application gracefully
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public static class PkcsToOrgsProcessor implements Processor<String, String, String, String> {

    private KeyValueStore<String, List<String>> kvStore;
    private ProcessorContext<String, String> context;

    @Override
    public void init(final ProcessorContext<String, String> context) {
//      context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
//        try (final KeyValueIterator<String, List<String>> iter = kvStore.all()) {
//          while (iter.hasNext()) {
//            final KeyValue<String, List<String>> entry = iter.next();
//            context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
//          }
//        }
//      });
      kvStore = context.getStateStore("OrgAggregations");
      this.context = context;
    }

    @Override
    public void process(final Record<String, String> record) {
      System.out.printf("Processing record with key:%s and value:%s%n",  record.key(), record.value());
      final List<String> orgList = kvStore.get(record.key());

      if (orgList == null) {
        System.out.printf("Creating entry for pkc: %s with org: %s%n", record.key(), record.value());
        List<String> initList = new ArrayList<>();
        initList.add(record.value());
        kvStore.put(record.key(), initList);
        context.forward(new Record<>(record.key(), initList.toString(), record.timestamp()));
      } else {
        System.out.printf("Found entry for pkc %s: %s%n", record.key(), orgList);
        if (!orgList.contains(record.value())) {
          orgList.add(record.value());
          kvStore.put(record.key(), orgList);
          context.forward(new Record<>(record.key(), orgList.toString(), record.timestamp()));
        }
      }
    }

    @Override
    public void close() {
      // close any resources managed by this processor
      // Note: Do not close any StateStores as these are managed by the library
    }
  }

  public static class NewKeyProcessor implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
      this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
      String oldKey = record.key();
      String oldValue = record.value();

      Record<String, String> newRecord = new Record<>(oldValue, oldKey, record.timestamp());

      // Forward the record with the new key
      context.forward(newRecord);
    }

    @Override
    public void close() {
      // Clean up resources if necessary
    }
  }
}
