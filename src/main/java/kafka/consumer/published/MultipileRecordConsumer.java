package kafka.consumer.published;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface MultipileRecordConsumer<KEY, VALUE> {

    void consumeRecords(ConsumerRecords<KEY, VALUE> records);
}
