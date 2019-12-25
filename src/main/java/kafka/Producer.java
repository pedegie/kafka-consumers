package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    private final KafkaProducer<String, String> producer;

    public Producer() {
        Properties kafkaProprs = new Properties();
        kafkaProprs.put("bootstrap.servers", "localhost:9092");
        kafkaProprs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProprs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(kafkaProprs);
    }


    public void send(String message)  {
        ProducerRecord<String, String> record = new ProducerRecord<>("TutorialTopic", message);

        try {
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
