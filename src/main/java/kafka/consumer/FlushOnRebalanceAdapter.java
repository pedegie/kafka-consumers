package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@FunctionalInterface
interface FlushOnRebalanceAdapter extends ConsumerRebalanceListener {


    @Override
    void onPartitionsRevoked(Collection<TopicPartition> collection);

    @Override
    default void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }
}
