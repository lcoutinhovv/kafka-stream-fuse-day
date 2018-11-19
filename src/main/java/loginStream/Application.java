package loginStream;

import loginStream.serialization.JsonPOJODeserializer;
import loginStream.serialization.JsonPOJOSerializer;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * /**
 * bin/kafka-topics.sh --create --topic login-stats \
 * --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 */
public class Application {
    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

        final KafkaStreams streams = buildLoginFeed(bootstrapServers, schemaRegistryUrl);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private static KafkaStreams buildLoginFeed(final String bootstrapServers, final String schemaRegistryUrl
    ) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "login-stats-app1");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        Serde<LoginData> loginDataSerde = getLoginDataSerde();


        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, LoginData> source = builder.stream("login-input1", Consumed.with(Serdes.String(), loginDataSerde));



        KTable<Windowed<String>, Long> counts = source
                .filter((key, value) -> value != null)
                .map((key, value) -> new KeyValue<>(value.getUserName(), value))
                .groupByKey(Serialized.with(Serdes.String(), loginDataSerde))
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(1)))
                .count();


        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        counts.toStream((windowed, count) ->
                "user:"+windowed.key()+":"+windowed.toString())
                .to("login-stats", Produced.with(stringSerde, longSerde));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    private static Serde<LoginData> getLoginDataSerde() {
        // TODO: the following can be removed with a serialization factory
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<LoginData> loginDataSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", LoginData.class);
        loginDataSerializer.configure(serdeProps, false);

        final Deserializer<LoginData> loginDataDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", LoginData.class);
        loginDataDeserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(loginDataSerializer, loginDataDeserializer);
    }


    public static class LoginData {
        String userName;
        String userPassword;
        String ip;
        Long date;

        public LoginData() {
        }

        public LoginData(String userName, String userPassword, String ip, Long date) {
            this.userName = userName;
            this.userPassword = userPassword;
            this.ip = ip;
            this.date = date;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getUserPassword() {
            return userPassword;
        }

        public void setUserPassword(String userPassword) {
            this.userPassword = userPassword;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public Long getDate() {
            return date;
        }

        public void setDate(Long date) {
            this.date = date;
        }
    }
}
