package kafka.consumer.published;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface SingleRecordConsumer<KEY, VALUE> {

    void consumeSingleRecord(ConsumerRecord<KEY, VALUE> record);
}
