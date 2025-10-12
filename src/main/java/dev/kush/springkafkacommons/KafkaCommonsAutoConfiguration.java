package dev.kush.springkafkacommons;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring Boot auto-configuration for Kafka commons library.
 * <p>
 * This configuration class automatically sets up essential Kafka components including:
 * <ul>
 *   <li>Producer factory with JSON serialization</li>
 *   <li>Consumer factory with JSON deserialization</li>
 *   <li>Kafka template for sending messages</li>
 *   <li>Listener container factory with error handling and retry logic</li>
 * </ul>
 * <p>
 * All beans are conditionally created only if they don't already exist in the application context,
 * allowing applications to override any configuration as needed.
 * <p>
 * The configuration uses properties from {@link KafkaCommonsProperties} to customize the Kafka setup.
 * Error handling includes automatic retry with exponential backoff and dead letter queue routing
 * based on exception types using {@link RecovererUtil}.
 *
 * @author Kush Parsaniya
 * @since 0.0.1
 * @see KafkaCommonsProperties
 * @see RecovererUtil
 */
@Configuration
@EnableConfigurationProperties(KafkaCommonsProperties.class)
public class KafkaCommonsAutoConfiguration {

    private final KafkaCommonsProperties props;
    private final KafkaTemplate<String, String> providedTemplateOptional; // optional constructor injection

    /**
     * Creates a new Kafka commons auto-configuration instance.
     *
     * @param props the Kafka commons properties for configuration
     */
    public KafkaCommonsAutoConfiguration(KafkaCommonsProperties props) {
        this.props = props;
        this.providedTemplateOptional = null;
    }

    /**
     * Creates a Kafka producer factory configured for string keys and JSON values.
     * <p>
     * The producer is configured with:
     * <ul>
     *   <li>String serializer for keys</li>
     *   <li>JSON serializer for values</li>
     *   <li>Bootstrap servers from properties</li>
     *   <li>Client ID with "-producer" suffix</li>
     * </ul>
     *
     * @return configured producer factory for Kafka messages
     */
    @Bean
    @ConditionalOnMissingBean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, props.clientId() + "-producer");
        return new DefaultKafkaProducerFactory<>(configs);
    }

    /**
     * Creates a Kafka template for sending messages.
     * <p>
     * The template is configured with the producer factory and can be used
     * to send messages to Kafka topics with JSON serialization.
     *
     * @param pf the producer factory to use for creating producers
     * @return configured Kafka template for sending messages
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }

    /**
     * Creates a Kafka consumer factory configured for string keys and JSON values.
     * <p>
     * The consumer is configured with:
     * <ul>
     *   <li>String deserializer for keys</li>
     *   <li>JSON deserializer for values with trusted packages set to "*"</li>
     *   <li>Bootstrap servers from properties</li>
     *   <li>Consumer group ID from properties</li>
     *   <li>Auto offset reset to "earliest"</li>
     *   <li>Client ID from properties</li>
     * </ul>
     *
     * @return configured consumer factory for Kafka messages
     */
    @Bean
    @ConditionalOnMissingBean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, props.clientId());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    /**
     * Creates a concurrent Kafka listener container factory with error handling and retry logic.
     * <p>
     * The factory is configured with:
     * <ul>
     *   <li>Consumer factory for creating consumers</li>
     *   <li>Concurrency level from properties</li>
     *   <li>Reply template for responses</li>
     *   <li>Dead letter publishing recoverer for failed messages</li>
     *   <li>Fixed backoff retry strategy with configurable interval and max attempts</li>
     * </ul>
     * <p>
     * Error handling includes automatic retry based on the configured interval and max attempts.
     * When max attempts are reached, messages are routed to appropriate dead letter queues
     * based on exception type using {@link RecovererUtil#getDlqTopicPartition}.
     *
     * @param consumerFactory the consumer factory for creating consumers
     * @param kafkaTemplate   the Kafka template for dead letter publishing
     * @return configured listener container factory with error handling
     * @see RecovererUtil#getDlqTopicPartition
     */
    @Bean
    @ConditionalOnMissingBean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTemplate<String, String> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.consumerConcurrency());
        factory.setReplyTemplate(kafkaTemplate);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, RecovererUtil::getDlqTopicPartition);

        // FixedBackOff(retryInterval, maxAttempts) - maxAttempts is number of retries, null means infinite.
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(props.retryIntervalMs(), props.retryMaxAttempts())));
        return factory;
    }
}
