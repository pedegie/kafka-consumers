package kafka.consumer;

import kafka.consumer.published.SingleRecordConsumer;
import kafka.consumer.storage.Storage;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;

final class AsyncExactlyOnceDeliverySubscribable<T, KEY, VALUE> extends AbstractKafkaConsumer<KEY, VALUE>  {

    private final Storage<T> storage;
    private final SingleRecordConsumer<KEY, VALUE> singleRecordConsumer;

    public AsyncExactlyOnceDeliverySubscribable(KafkaConsumer<KEY, VALUE> kafkaConsumer,
                                                SingleRecordConsumer<KEY, VALUE> singleRecordConsuming,
                                                Storage<T> storage,
                                                Set<String> topics) {
        super(kafkaConsumer, topics);
        this.singleRecordConsumer = singleRecordConsuming;
        this.storage = storage;
        kafkaConsumer.subscribe(topics, new SaveOffsetOnRebalance(kafkaConsumer));
    }

    @Override
    public void subscribe() {
        kafkaConsumer.poll(Duration.ZERO);

        for(TopicPartition partition : kafkaConsumer.assignment()) {
            kafkaConsumer.seek(partition, storage.getOffsetForPartition(partition));
        }

        while(true) {
            ConsumerRecords<KEY, VALUE> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<KEY, VALUE> record : records)
            {
                singleRecordConsumer.consumeSingleRecord(record);
                storage.save(record);
                storage.storeOffset(record);
            }
            storage.commitTransaction();
        }
    }

    private class SaveOffsetOnRebalance implements ConsumerRebalanceListener {

        KafkaConsumer<KEY, VALUE> kafkaConsumer;

        public SaveOffsetOnRebalance(KafkaConsumer<KEY, VALUE> kafkaConsumer) {
            this.kafkaConsumer = kafkaConsumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            storage.commitTransaction();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            for (TopicPartition partition : collection) {
                kafkaConsumer.seek(partition, storage.getOffsetForPartition(partition));
            }

        }
    }
}
