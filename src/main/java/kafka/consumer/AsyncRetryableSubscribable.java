package kafka.consumer;

import kafka.consumer.published.MultipileRecordConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

final class AsyncRetryableSubscribable<KEY, VALUE>  extends AbstractKafkaConsumer<KEY, VALUE> implements Flushable {

    private final MultipileRecordConsumer<KEY, VALUE> recordConsumer;

    private final LongAdder commitMark = new LongAdder();
    int retryCount = 2;

    public AsyncRetryableSubscribable(KafkaConsumer<KEY, VALUE> kafkaConsumer, MultipileRecordConsumer<KEY, VALUE> recordConsumer, Set<String> topics) {
        super(kafkaConsumer, topics);
        this.recordConsumer = recordConsumer;
        kafkaConsumer.subscribe(topics, new FlushOnRebalanceListener(this));
    }

    @Override
    public void subscribe() {
        while (true) {
            ConsumerRecords<KEY, VALUE> records = kafkaConsumer.poll(Duration.ofMillis(100));
            recordConsumer.consumeRecords(records);
            long mark = commitMark.longValue();
            kafkaConsumer.commitAsync((partitions, exc) -> retryOnError(exc, mark, kafkaConsumer, 1));
        }
    }

    private void retryOnError(Exception exc, long mark, KafkaConsumer<KEY, VALUE> kafkaConsumer, int retry) {
        if (exc == null) {
            commitMark.increment();
        } else {
            exceptionHandler.accept(exc);
            if (retry < retryCount && newerCommitWasNotSentInMeanwhile(mark)) {
                kafkaConsumer.commitAsync((partitions, exception) -> retryOnError(exception, mark, kafkaConsumer, retry + 1));
            }
        }
    }

    private boolean newerCommitWasNotSentInMeanwhile(long mark) {
        return commitMark.longValue() == mark;
    }

    @Override
    public void flush() {
        kafkaConsumer.commitSync();
    }
}
