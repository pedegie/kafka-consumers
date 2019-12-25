package kafka.consumer.published;

import org.apache.kafka.clients.consumer.ConsumerRecords;

@FunctionalInterface
public interface MultipleRecordConsumer<KEY, VALUE> {

    void consumeRecords(ConsumerRecords<KEY, VALUE> records);
}
