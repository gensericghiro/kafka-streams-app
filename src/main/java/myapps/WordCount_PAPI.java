package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;

public class WordCount_PAPI {
  private static final String inputTopic = "envs";
  private static final String outputTopic = "envs_to_orgs";

  public static void main(String[] args) {
    Properties props = new Properties();

    try {
      props = Example.readConfig("client.properties");
    } catch (IOException e) {
      e.printStackTrace();
    }

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-processor-app");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    Topology topology = new Topology();

    // 1. Add a source node to read from an input topic
    topology.addSource("Source", inputTopic);

    // 2. Define a state store for word counts
    topology.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("Counts"),
        Serdes.String(),
        Serdes.Integer()), "Process"); // Associate with the "Process" node

    // 3. Add a processor node with custom logic and state store access
    topology.addProcessor("Process", () -> new WordCountProcessor(), "Source");

    // 4. Add a sink node to write results to an output topic
    topology.addSink("Sink", outputTopic, "Process");

    KafkaStreams streams = new KafkaStreams(topology, props);

    WordCount_DSL.startProduceCronjob(inputTopic, props);
    WordCount_DSL.startProduceCronjob(outputTopic, props);

    streams.start();

    // Add shutdown hook to close the Streams application gracefully
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public static class WordCountProcessor implements Processor<String, String, String, String> {

    private KeyValueStore<String, Integer> kvStore;

    @Override
    public void init(final ProcessorContext<String, String> context) {
      context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
        try (final KeyValueIterator<String, Integer> iter = kvStore.all()) {
          while (iter.hasNext()) {
            final KeyValue<String, Integer> entry = iter.next();
            context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
          }
        }
      });
      kvStore = context.getStateStore("Counts");
    }

    @Override
    public void process(final Record<String, String> record) {
      final String[] words = record.value().toLowerCase(Locale.getDefault()).split("\\W+");

      for (final String word : words) {
        final Integer oldValue = kvStore.get(word);

        if (oldValue == null) {
          kvStore.put(word, 1);
        } else {
          kvStore.put(word, oldValue + 1);
        }
      }
    }

    @Override
    public void close() {
      // close any resources managed by this processor
      // Note: Do not close any StateStores as these are managed by the library
    }
  }
}
