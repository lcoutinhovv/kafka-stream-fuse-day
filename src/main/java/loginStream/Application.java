package loginStream;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import loginStream.avroSchema.AvroSchema;
import loginStream.serialization.JsonPOJODeserializer;
import loginStream.serialization.JsonPOJOSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * /**
 * bin/kafka-topics.sh --create --topic persons-avro5\
 * --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 */
public class Application {

    private static final String TOPIC = "attack";
    private static final String INPUT_TOPIC = "login-input-30";
    private static final String SCHEMA_URL = "http://localhost:8081";
    private static final String SCHEMA_PATH = "src/main/java/loginStream/login-attack-count.avsc";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(final String[] args) throws IOException, RestClientException {
        AvroSchema.registerSchema(TOPIC, SCHEMA_PATH, SCHEMA_URL);

        final KafkaStreams streams = buildLoginFeed();
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private static KafkaStreams buildLoginFeed() {
        final Properties streamsConfiguration = new Properties();
        String appId = "my-app-id";

        configureStream(streamsConfiguration, appId);

        Serde<LoginData> loginDataSerde = getLoginDataSerde(LoginData.class);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, LoginData> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), loginDataSerde));


        KTable<Windowed<String>, Long> counts = source
                .filter((key, value) -> value != null)
                .map((key, value) -> new KeyValue<>(value.getUserName(), value))
                .groupByKey(Serialized.with(Serdes.String(), loginDataSerde))
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(1)))
                .count();


        final Serde<String> stringSerde = Serdes.String();

        Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
        boolean isKeySerde = false;
        genericAvroSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        "http://localhost:8081"),
                isKeySerde);

        counts.mapValues((windowed, counter) ->
        {
            try {
                return buildRecord(new LoginAttackCount(
                        windowed.key(),
                        counter,
                        windowed.window().start(),
                        windowed.window().end()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        })
                .toStream()
                .selectKey((k, v) -> k.toString())
                .to(TOPIC, Produced.with(stringSerde, genericAvroSerde));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    private static void configureStream(Properties streamsConfiguration, String appId) {
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    }

    private static <T> Serde<T> getLoginDataSerde(Class<T> clazz) {
        // TODO: the following can be removed with a serialization factory
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<T> loginDataSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", clazz);
        loginDataSerializer.configure(serdeProps, false);

        final Deserializer<T> loginDataDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", clazz);
        loginDataDeserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(loginDataSerializer, loginDataDeserializer);
    }

    private static GenericRecord buildRecord(LoginAttackCount loginAttackCount) throws IOException {
        // avro schema avsc file path.
        String schemaPath = "src/main/java/loginStream/login-attack-count.avsc";

        String schemaString;

        try (FileInputStream inputStream = new FileInputStream(schemaPath)) {
            schemaString = org.apache.commons.io.IOUtils.toString(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Schema schema = new Schema.Parser().parse(schemaString);
        GenericData.Record record = new GenericData.Record(schema);
        // put the elements according to the avro schema.
        record.put("userName", loginAttackCount.userName);
        record.put("counter", loginAttackCount.counter);
        record.put("to", loginAttackCount.to);
        record.put("start", loginAttackCount.start);

        return record;
    }


}
