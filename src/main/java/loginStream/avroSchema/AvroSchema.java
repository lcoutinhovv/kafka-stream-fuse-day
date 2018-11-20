package loginStream.avroSchema;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;

public class AvroSchema {
    public static void registerSchema(String topic,String schemaPath, String schemaUrl) throws IOException, RestClientException {

        // subject convention is "<topic-name>-value"
        String subject = topic + "-value";

        String schema;

        FileInputStream inputStream = new FileInputStream(schemaPath);
        try {
            schema = IOUtils.toString(inputStream);
        } finally {
            inputStream.close();
        }

        Schema avroSchema = new Schema.Parser().parse(schema);

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(schemaUrl, 20);

        client.register(subject, avroSchema);
    }
}
