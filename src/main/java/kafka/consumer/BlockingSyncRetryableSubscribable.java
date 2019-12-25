package kafka.consumer;

import kafka.consumer.published.MultipleRecordConsumer;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Set;

final class BlockingSyncRetryableSubscribable<KEY, VALUE>  extends AbstractKafkaConsumer<KEY, VALUE> {

    private final MultipleRecordConsumer<KEY, VALUE> recordConsumer;

    public BlockingSyncRetryableSubscribable(KafkaConsumer<KEY, VALUE> kafkaConsumer, MultipleRecordConsumer<KEY, VALUE> recordConsumer, Set<String> topic) {
        super(kafkaConsumer, topic);
        this.recordConsumer = recordConsumer;
        kafkaConsumer.subscribe(topic, new FlushOnRebalanceListener(this));
    }

    @Override
    public void subscribe() {
        while(true) {
            ConsumerRecords<KEY, VALUE> records = kafkaConsumer.poll(Duration.ofMillis(100));
            recordConsumer.consumeRecords(records);
            try {
                kafkaConsumer.commitSync();
            } catch (CommitFailedException e) {
                exceptionHandler.accept(e);
            }
        }
    }

    @Override
    public void flush() {
        kafkaConsumer.commitSync();
    }
}
