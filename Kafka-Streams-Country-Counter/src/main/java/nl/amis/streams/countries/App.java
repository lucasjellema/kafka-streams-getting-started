package nl.amis.streams.countries;

import nl.amis.streams.JsonPOJOSerializer;
import nl.amis.streams.JsonPOJODeserializer;

// generic Java imports
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
// Kafka imports
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
// Kafka Streams related imports
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

public class App {
    static public class CountryMessage {
        /* the JSON messages produced to the countries Topic have this structure:
         { "name" : "The Netherlands"
         , "code" : "NL
         , "continent" : "Europe"
         , "population" : 17281811
         , "size" : 42001
         };
  
        this class needs to have at least the corresponding fields to deserialize the JSON messages into
        */

        public String code;
        public String name;
        public int population;
        public int size;
        public String continent;
    }

    private static final String APP_ID = "countries-streaming-analysis-app";

    public static void main(String[] args) {
        System.out.println("Kafka Streams Demonstration");

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());
        final Serde < String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();

        // define countryMessageSerde
        Map < String, Object > serdeProps = new HashMap < > ();
        final Serializer < CountryMessage > countryMessageSerializer = new JsonPOJOSerializer < > ();
        serdeProps.put("JsonPOJOClass", CountryMessage.class);
        countryMessageSerializer.configure(serdeProps, false);

        final Deserializer < CountryMessage > countryMessageDeserializer = new JsonPOJODeserializer < > ();
        serdeProps.put("JsonPOJOClass", CountryMessage.class);
        countryMessageDeserializer.configure(serdeProps, false);
        final Serde < CountryMessage > countryMessageSerde = Serdes.serdeFrom(countryMessageSerializer, countryMessageDeserializer);

        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        // the source of the streaming analysis is the topic with country messages
        KStream<String, CountryMessage> countriesStream = 
                                       kStreamBuilder.stream(stringSerde, countryMessageSerde, "countries");

        // THIS IS THE CORE OF THE STREAMING ANALYTICS:
        // running count of countries per continent, published in topic RunningCountryCountPerContinent
        KTable<String,Long> runningCountriesCountPerContinent = countriesStream
                                                                 .selectKey((k, country) -> country.continent)
                                                                 .countByKey("Counts")
                                                                 ;
        runningCountriesCountPerContinent.to(stringSerde, longSerde,  "RunningCountryCountPerContinent");
        runningCountriesCountPerContinent.print(stringSerde, longSerde);



        System.out.println("Starting Kafka Streams Countries Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();
        System.out.println("Now started CountriesStreams Example");
    }

    private static Properties getProperties() {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        // Kafka bootstrap server (broker to talk to); ubuntu is the host name for my VM running Kafka, port 9092 is where the (single) broker listens 
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
        // Apache ZooKeeper instance keeping watch over the Kafka cluster; ubuntu is the host name for my VM running Kafka, port 2181 is where the ZooKeeper listens 
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "ubuntu:2181");
        // default serdes for serialzing and deserializing key and value from and to streams in case no specific Serde is specified
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\temp");
        // to work around exception Exception in thread "StreamThread-1" java.lang.IllegalArgumentException: Invalid timestamp -1
        // at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:60)
        // see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return settings;
    }

}