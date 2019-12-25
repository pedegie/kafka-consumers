package kafka.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

final class FlushOnRebalanceListener implements FlushOnRebalanceAdapter {

    private final Flushable flushable;

    public FlushOnRebalanceListener(Flushable flushable) {
        this.flushable = flushable;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        flushable.flush();
    }
}
