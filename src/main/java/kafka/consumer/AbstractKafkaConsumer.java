package kafka.consumer;

import kafka.consumer.published.Subscribable;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Set;
import java.util.function.Consumer;

abstract class AbstractKafkaConsumer<KEY, VALUE> implements Subscribable, Flushable {

    protected final Set<String> topicList;
    protected final java.util.function.Consumer<Exception> exceptionHandler;
    protected final KafkaConsumer<KEY, VALUE> kafkaConsumer;


    protected AbstractKafkaConsumer(KafkaConsumer<KEY, VALUE> kafkaConsumer, Set<String> topicList) {
        this.kafkaConsumer = kafkaConsumer;
        this.topicList = topicList;
        this.exceptionHandler = defaultExceptionHandler();
    }

    protected Consumer<Exception> defaultExceptionHandler() {
        return Throwable::printStackTrace;
    }
}
