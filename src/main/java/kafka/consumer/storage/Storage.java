package kafka.consumer.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public interface Storage<T> {
    T store (T t);

    <VALUE, KEY> void storeOffset(ConsumerRecord<KEY, VALUE> record);

    <VALUE, KEY> void save(ConsumerRecord<KEY, VALUE> record);

    void commitTransaction();

    long getOffsetForPartition(TopicPartition partition);
}
