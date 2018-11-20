### kafka-stream-fuse-day-Tikal
This Repository is a Kafka Stream example that read data from one topic, do some aggregation, and write this in Avro format to output topic. the elasticsearch connect can send this to elasticsearch and create a matching topic.

#quick scan on its work:

read data from input topic in this format:
(to have this input you can pull https://github.com/brachi-wernick/kafka-login-producer and follow steps there)
```java
public class LoginData {
    String userName;
    String userPassword;
    String ip;
    Long date;
}
```
Configure a SerDe for LoginData:
look in this function loginStream.Application.getLoginDataSerde
this base on [Confluent example](https://github.com/confluentinc/kafka-streams-examples/blob/5.0.1-post/src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java)

build the stream:
```java
final KStream<String, LoginData> source = 
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), loginDataSerde));
```

do some aggregation 

```java

KTable<Windowed<String>, Long> counts = source
        .filter((key, value) -> value != null)
        .map((key, value) -> new KeyValue<>(value.getUserName(), value))
        .groupByKey(Serialized.with(Serdes.String(), loginDataSerde))
        .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(1)))
        .count();
```

create Avro Record, different schema, including the aggregation and time slicing from previous step
see method `loginStream.Application.buildRecord`

In order to cause make this happen, you need to register schema. look in class `loginStream.avroSchema.AvroSchema` which handle schema registration.
this can be done also in the CLI:
for example
```
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{ "schema": "{ \"type\": \"record\", \"name\": \"Persone\”, \"namespace\": \"com.ippontech.kafkatutorialse\”, \"fields\": [ { \"name\": \"firstName\", \"type\": \"string\" }, { \"name\": \"lastName\", \"type\": \"string\" }, { \"name\": \"birthDate\", \"type\": \"long\" } ]}" }' \
  http://localhost:8081/subjects/persons-avro-value/versions
```

And then, write this to output topic 
```java
.to(TOPIC, Produced.with(stringSerde, genericAvroSerde));
```

If topic isn't create automatically run:
```
bin/kafka-topics.sh --create --topic attack\
 --zookeeper localhost:2181 --partitions 1 --replication-factor 1
```

Finally this could be integrated easily with Elasticsearch
by running 
```
  ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties \
etc/kafka-connect-elasticsearch/quickstart-elasticsearch.properties
```

just make sure to update connect-avro-standalone.properties

```properties
key.converter=org.apache.kafka.connect.storage.StringConverter
internal.key.converter=org.apache.kafka.connect.storage.StringConverter
```

and not, because key is string
```properties
key.converter=io.confluent.connect.avro.AvroConverter
```

to see if topic was deployed, run:
```
curl -XGET 'http://localhost:9200/test-elasticsearch-sink/_search?pretty'
```


:
![Here are nice dashboard I did in Kibana](src/main/resources/kibana-dashboard.png)


##To run teh demo E2E

To run the demo end to end

1. Git pull 
https://github.com/brachi-wernick/kafka-login-producer

https://github.com/brachi-wernick/kafka-stream-fuse-day

2. Download Kafka https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz
3. Start zookeeper ./bin/zookeeper-server-start.sh config/zookeeper.properties
4. Start Kafka server  ./bin/kafka-server-start.sh config/server.properties
5. Start schema regsitration in Confluent
./bin/schema-registry-start etc/schema-registry/schema-registry.properties
6. Download elastic and start it: ./bin/elasticsearch
7. Start Kibana ./bin/kibana 
8. Run kafka-login-producer, and send login request in this format:
            {
              "username":"Foo",
              "password":"567"
            }
9. Run kafka-stream-fuse-day 
10. Connect to elastic from confluent
./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties \
etc/kafka-connect-elasticsearch/quickstart-elasticsearch.properties