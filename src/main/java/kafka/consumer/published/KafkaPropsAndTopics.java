package kafka.consumer.published;

import java.util.Properties;
import java.util.Set;

final public class KafkaPropsAndTopics {
    private final Properties props;
    private final Set<String> topics;

    public KafkaPropsAndTopics(Properties props, Set<String> topics) {
        this.props = props;
        this.topics = topics;
    }

    public Properties getProps() {
        return props;
    }

    public Set<String> getTopics() {
        return topics;
    }
}
