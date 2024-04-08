
import com.wayfair.partner.offers.catalog.generated.events.CurrentStatus;
import com.wayfair.partner.offers.catalog.generated.events.OfferStatusUpdatedEvent;
import com.wayfair.partner.offers.catalog.generated.events.PreviousStatus;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import java.util.UUID;
import lombok.val;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import static com.wayfair.partner.offers.catalog.generated.events.Status.ACTIVATED;
import static com.wayfair.partner.offers.catalog.generated.events.Status.DEACTIVATED;


public class KafkaEventProducer {

    private final String bootstrapServers;
    private final String topic;

    public KafkaEventProducer(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    public void sendEvent(OfferStatusUpdatedEvent event) {
        // Configure Kafka producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        properties.setProperty("schema.registry.url", "http://kube-kafka-schema-c1.service.intradsm1.sdeconsul.csnzoo.com");

        // Create Kafka producer
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        // Create a ProducerRecord with the Avro GenericRecord
        ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topic, event);

        // Send the record to Kafka
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Event sent successfully: " + event);
            } else {
                System.err.println("Error sending event: " + event);
                exception.printStackTrace();
            }
        });

        // Flush and close the producer
        producer.flush();
        producer.close();
    }

    public static void main(String[] args) {
        // Example usage
        String bootstrapServers = "c14.kafka-broker.service.intradsm1.sdeconsul.csnzoo.com:9092";
        String topic = "partner-service-offer";
        KafkaEventProducer producer = new KafkaEventProducer(bootstrapServers, topic);
        val event = OfferStatusUpdatedEvent.newBuilder()
                .setUserId("669115")
                .setSalesAccountId("72b8cb17-16a1-4968-9a08-536a75bb4d74")
                .setOfferId("c7ad0a6c-f2d6-47d9-84d6-7f55d0a6328e")
                .setCurrent(CurrentStatus.newBuilder().setStatus(ACTIVATED).build())
                .setPrevious(PreviousStatus.newBuilder().setStatus(DEACTIVATED).build())
                .build();
        producer.sendEvent(event);
    }
}
