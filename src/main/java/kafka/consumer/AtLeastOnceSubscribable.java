package kafka.consumer;

import kafka.consumer.published.MultipileRecordConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Set;

final class AtLeastOnceSubscribable<KEY, VALUE> extends AbstractKafkaConsumer<KEY, VALUE> implements Flushable   {

    private static final int RETRY_UP_TO = 100;
    private final RetryableAsyncSubscribable<KEY, VALUE> retryable;

    protected AtLeastOnceSubscribable(KafkaConsumer<KEY, VALUE> kafkaConsumer, MultipileRecordConsumer<KEY, VALUE> consumer, Set<String> topicList) {
        super(kafkaConsumer, topicList);
        this.retryable = new RetryableAsyncSubscribable<>(kafkaConsumer, consumer, topicList, RETRY_UP_TO);
    }

    @Override
    public void subscribe() {
        retryable.subscribe();
    }

    @Override
    public void flush() {
        kafkaConsumer.commitSync();
    }
}
