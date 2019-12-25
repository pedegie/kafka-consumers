package kafka.consumer;

import kafka.consumer.published.MultipileRecordConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Set;

final class AsyncSubscribable<KEY, VALUE> extends AbstractKafkaConsumer<KEY, VALUE> implements Flushable {

    private final MultipileRecordConsumer<KEY, VALUE> consumerRecords;

    public AsyncSubscribable(KafkaConsumer<KEY, VALUE> kafkaConsumer, MultipileRecordConsumer<KEY, VALUE> consumerRecords, Set<String> topics) {
        super(kafkaConsumer, topics);
        this.consumerRecords = consumerRecords;
        kafkaConsumer.subscribe(topics, new FlushOnRebalanceListener(this));
    }

    @Override
    public void subscribe() {
        while (true) {
            ConsumerRecords<KEY, VALUE> records = kafkaConsumer.poll(Duration.ofMillis(100));
            consumerRecords.consumeRecords(records);
            kafkaConsumer.commitAsync((map, e) -> exceptionHandler.accept(e));
        }
    }

    @Override
    public void flush() {
        kafkaConsumer.commitSync();
    }
}
