package myapps;

import org.apache.kafka.common.serialization.*;
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
  private static final String inputTopic = "word_stream_in";
  private static final String outputTopic = "word_count_out";

  private static final StringSerializer stringSerializer = new StringSerializer();
  private static final LongSerializer longSerializer = new LongSerializer();

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

    // 2. Add a processor node with custom logic and state store access
    topology.addProcessor("Process", () -> new WordCountProcessor(), "Source");

    // 3. Define a state store for word counts
    topology.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("Counts"),
        Serdes.String(),
        Serdes.Long()), "Process"); // Associate with the "Process" node

    // 4. Add a sink node to write results to an output topic
    topology.addSink("Sink", outputTopic, stringSerializer, longSerializer, "Process");
//    topology.addSink("Sink", outputTopic, stringSerializer, longSerializer, "Process");

    KafkaStreams streams = new KafkaStreams(topology, props);

    WordCount_DSL.startProduceCronjob(inputTopic, props);
    WordCount_DSL.startConsumeCronjob(outputTopic, props);

    try {
      streams.cleanUp();
      streams.start();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Add shutdown hook to close the Streams application gracefully
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public static class WordCountProcessor implements Processor<String, String, String, Long> {

//    private KeyValueStore<String, Long> kvStore;
    private KeyValueStore<String, Long> kvStore;

    @Override
    public void init(final ProcessorContext<String, Long> context) {
      context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
        try (final KeyValueIterator<String, Long> iter = kvStore.all()) {
//          try (final KeyValueIterator<String, Long> iter = kvStore.all()) {
          while (iter.hasNext()) {
            final KeyValue<String, Long> entry = iter.next();
            context.forward(new Record<>(entry.key, entry.value, timestamp));
          }
        }
      });
      kvStore = context.getStateStore("Counts");
    }

    @Override
    public void process(final Record<String, String> record) {
      record.withValue(record.value());
      final String[] words = record.value().toLowerCase(Locale.getDefault()).split("\\W+");

      for (final String word : words) {
        final Long oldValue = kvStore.get(word);
//        final Long oldValue = kvStore.get(word);

        if (oldValue == null) {
          System.out.println("Creating entry for word: " + word);
//          kvStore.put(word, (long) 1);
          kvStore.put(word, (long) 1);
        } else {
          System.out.println("Found entry for word: " + oldValue);
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
