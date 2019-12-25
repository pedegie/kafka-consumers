package kafka.consumer;

import kafka.consumer.published.MultipleRecordConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

final class RetryableAsyncSubscribable<KEY, VALUE>  extends AbstractKafkaConsumer<KEY, VALUE>  {

    private final MultipleRecordConsumer<KEY, VALUE> recordConsumer;

    private final LongAdder commitMark = new LongAdder();
    private final int retryCount;

    public RetryableAsyncSubscribable(KafkaConsumer<KEY, VALUE> kafkaConsumer, MultipleRecordConsumer<KEY, VALUE> recordConsumer, Set<String> topics, int retryCount) {
        super(kafkaConsumer, topics);
        this.recordConsumer = recordConsumer;
        this.retryCount = retryCount;
        kafkaConsumer.subscribe(topics, new FlushOnRebalanceListener(this));
    }

    @Override
    public void subscribe() {
        while (true) {
            ConsumerRecords<KEY, VALUE> records = kafkaConsumer.poll(Duration.ofMillis(100));
            recordConsumer.consumeRecords(records);
            long mark = commitMark.longValue();
            kafkaConsumer.commitAsync((partitions, exc) -> retryOnError(exc, mark, 1));
        }
    }

    private void retryOnError(Exception exc, long mark, int retry) {
        if (exc == null) {
            commitMark.increment();
        } else {
            exceptionHandler.accept(exc);
            if (retry < retryCount && newerCommitWasNotSentInMeanwhile(mark)) {
                kafkaConsumer.commitAsync((partitions, exception) -> retryOnError(exception, mark, retry + 1));
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
