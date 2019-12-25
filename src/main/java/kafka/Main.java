package kafka;

import kafka.consumer.SubscribableFactory;
import kafka.consumer.published.KafkaPropsAndTopics;
import kafka.consumer.published.SingleRecordConsumer;

import java.util.Collections;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        KafkaPropsAndTopics props = prepareProps();

        SingleRecordConsumer<String, String> consumer = record -> System.out.println(record.key());

        SubscribableFactory.consumeWithAsyncBatch(props, consumer)
                .subscribe();
    }

    private static KafkaPropsAndTopics prepareProps() {
        Properties kafkaProprs = new Properties();
        kafkaProprs.put("bootstrap.servers", "localhost:9092");
        kafkaProprs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProprs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaPropsAndTopics(kafkaProprs, Collections.singleton("qweqe"));
    }

}
