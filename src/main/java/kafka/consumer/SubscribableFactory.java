package kafka.consumer;

import kafka.consumer.published.KafkaPropsAndTopics;
import kafka.consumer.published.MultipileRecordConsumer;
import kafka.consumer.published.SingleRecordConsumer;
import kafka.consumer.published.Subscribable;
import kafka.consumer.storage.Storage;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class SubscribableFactory {

    protected SubscribableFactory() {
    }

    public static <KEY, VALUE> Subscribable consumeWithAsyncBatch(KafkaPropsAndTopics props, SingleRecordConsumer<KEY, VALUE> consumer) {
        KafkaConsumer<KEY, VALUE> kafkaConsumer = prepareConsumer(props.getProps());
        return new FlushOnErrorDecorator<>(new AsyncBatchSubscribable<>(kafkaConsumer, consumer, props.getTopics()), kafkaConsumer);
    }

    public static <KEY, VALUE> Subscribable atMostOnceConsumer(KafkaPropsAndTopics props, MultipileRecordConsumer<KEY, VALUE> consumer) {
        KafkaConsumer<KEY, VALUE> kafkaConsumer = prepareConsumer(props.getProps());
        return new FlushOnErrorDecorator<>(new AtMostOnceSubscribable<>(kafkaConsumer, consumer, props.getTopics()), kafkaConsumer);

    }

    public static <KEY, VALUE> Subscribable atLeastOnceConsumer(KafkaPropsAndTopics props, MultipileRecordConsumer<KEY, VALUE> consumer) {
        KafkaConsumer<KEY, VALUE> kafkaConsumer = prepareConsumer(props.getProps());
        return new FlushOnErrorDecorator<>(new AtLeastOnceSubscribable<>(kafkaConsumer, consumer, props.getTopics()), kafkaConsumer);

    }

    public static <T, KEY, VALUE> Subscribable exactlyOnceConsumer(KafkaPropsAndTopics props,
                                                                            SingleRecordConsumer<KEY, VALUE> consumer,
                                                                            Storage<T> storage) {
        KafkaConsumer<KEY, VALUE> kafkaConsumer = prepareConsumer(props.getProps());
        return new FlushOnErrorDecorator<>(new ExactlyOnceSubscribable<>(kafkaConsumer, consumer, storage, props.getTopics()), kafkaConsumer);

    }

    public static <KEY, VALUE> Subscribable consumeWithAsyncRetryable(KafkaPropsAndTopics props,
                                                               MultipileRecordConsumer<KEY, VALUE> consumer,
                                                                      int retryCount) {
        KafkaConsumer<KEY, VALUE> kafkaConsumer = prepareConsumer(props.getProps());
        return new FlushOnErrorDecorator<>(new RetryableAsyncSubscribable<>(kafkaConsumer, consumer, props.getTopics(), retryCount), kafkaConsumer);
    }

    public static <KEY, VALUE> Subscribable consumeWithBlockingSyncRetryable(KafkaPropsAndTopics props,
                                                                      MultipileRecordConsumer<KEY, VALUE> consumer) {
        KafkaConsumer<KEY, VALUE> kafkaConsumer = prepareConsumer(props.getProps());
        return new FlushOnErrorDecorator<>(new BlockingSyncRetryableSubscribable<>(kafkaConsumer, consumer, props.getTopics()), kafkaConsumer);
    }


    private static <KEY, VALUE> KafkaConsumer<KEY, VALUE> prepareConsumer(Properties kafkaProps) {
        return null;
    }

}
