package kafka.consumer;

import kafka.consumer.published.Subscribable;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

final class FlushOnErrorDecorator<T extends Subscribable & Flushable , KEY, VALUE> implements Subscribable {

    private final T flushableConsumer;
    private final KafkaConsumer<KEY, VALUE> kafkaConsumer;

    public FlushOnErrorDecorator(T flushableConsumer, KafkaConsumer<KEY, VALUE> kafkaConsumer) {
        this.flushableConsumer = flushableConsumer;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void subscribe() {
        try {
            flushableConsumer.subscribe();
        }catch (WakeupException e) {
            // intentionally close
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                flushableConsumer.flush();
            } finally {
                kafkaConsumer.close();
                System.out.println("Consumer closed");
            }
        }
    }
}
