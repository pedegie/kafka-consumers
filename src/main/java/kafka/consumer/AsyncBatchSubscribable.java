package kafka.consumer;

import kafka.consumer.published.SingleRecordConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

final class AsyncBatchSubscribable<KEY, VALUE>  extends AbstractKafkaConsumer<KEY, VALUE> {

    private static final int BATCH_SIZE = 1000;
    private final SingleRecordConsumer<KEY, VALUE> consumer;
    int count = 0;

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public AsyncBatchSubscribable(KafkaConsumer<KEY, VALUE> kafkaConsumer, SingleRecordConsumer<KEY, VALUE> recordConsumer, Set<String> topicList) {
        super(kafkaConsumer, topicList);
        this.consumer = recordConsumer;
        kafkaConsumer.subscribe(topicList, new FlushOnRebalanceListener(this));
    }

    @Override
    public void subscribe() {
        while (true) {
            ConsumerRecords<KEY, VALUE> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<KEY, VALUE> record : records) {
                consumer.consumeSingleRecord(record);
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                if (count % BATCH_SIZE == 0)
                    kafkaConsumer.commitAsync(currentOffsets, null);
                count++;
            }
        }
    }

    @Override
    public void flush() {
        kafkaConsumer.commitAsync(currentOffsets, null);
    }
}
