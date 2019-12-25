package kafka;

import kafka.consumer.SubscribableFactory;
import kafka.consumer.published.KafkaPropsAndTopics;
import kafka.consumer.published.MultipleRecordConsumer;
import kafka.consumer.published.SingleRecordConsumer;
import kafka.consumer.storage.Storage;

import java.util.Collections;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        KafkaPropsAndTopics props = prepareProps();

        SingleRecordConsumer<String, String> consumer = record -> System.out.println(record.key());
        MultipleRecordConsumer<String, String> multipleRecordConsumer = record -> System.out.println(record.count());

        SubscribableFactory.consumeWithAsyncBatch(props, consumer).subscribe();
        SubscribableFactory.atMostOnceConsumer(props, multipleRecordConsumer).subscribe();
        SubscribableFactory.atLeastOnceConsumer(props, multipleRecordConsumer).subscribe();


        Storage storage = null; // some kind of persisted storage
        SubscribableFactory.exactlyOnceConsumer(props, consumer,storage).subscribe();

        SubscribableFactory.consumeWithBlockingSyncRetryable(props, multipleRecordConsumer).subscribe();

    }

    private static KafkaPropsAndTopics prepareProps() {
        Properties kafkaProprs = new Properties();
        kafkaProprs.put("bootstrap.servers", "localhost:9092");
        kafkaProprs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProprs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaPropsAndTopics(kafkaProprs, Collections.singleton("qweqe"));
    }

}
