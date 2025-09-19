package dev.kush.springkafkacommons;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
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

@Configuration
@EnableConfigurationProperties(KafkaCommonsProperties.class)
public class KafkaCommonsAutoConfiguration {

    private final KafkaCommonsProperties props;
    private final KafkaTemplate<String, Object> providedTemplateOptional; // optional constructor injection

    public KafkaCommonsAutoConfiguration(KafkaCommonsProperties props) {
        this.props = props;
        this.providedTemplateOptional = null;
    }

    @Bean
    @ConditionalOnMissingBean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, props.clientId() + "-producer");
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> pf) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, props.clientId());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configs.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    @ConditionalOnMissingBean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.consumerConcurrency());
        factory.setReplyTemplate(kafkaTemplate);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, this::topicPartition);

        // FixedBackOff(retryInterval, maxAttempts) - maxAttempts is number of retries, null means infinite.
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(props.retryIntervalMs(), props.retryMaxAttempts())));
        return factory;
    }

    TopicPartition topicPartition(ConsumerRecord<?, ?> record, Exception ex) {
        return switch (ex) {
            // Retryable exceptions
            case CannotAcquireLockException e ->
                    new TopicPartition(record.topic() + ".RETRY_DB_LOCK", record.partition());
            case PessimisticLockingFailureException e ->
                    new TopicPartition(record.topic() + ".RETRY_DB_PESSIMISTIC_LOCK", record.partition());
            case ConcurrencyFailureException e ->
                    new TopicPartition(record.topic() + ".RETRY_CONCURRENCY", record.partition());
            case TransientDataAccessResourceException e ->
                    new TopicPartition(record.topic() + ".RETRY_DB_TRANSIENT", record.partition());
            case QueryTimeoutException e ->
                    new TopicPartition(record.topic() + ".RETRY_DB_TIMEOUT", record.partition());
            case KafkaException e -> new TopicPartition(record.topic() + ".RETRY_KAFKA", record.partition());

            // Non-retryable → straight to DLQ
            case IllegalArgumentException e ->
                    new TopicPartition(record.topic() + ".ILLEGAL_ARGUMENT", record.partition());
            case NullPointerException e -> new TopicPartition(record.topic() + ".NULL_POINTER", record.partition());
            case RuntimeException e -> new TopicPartition(record.topic() + ".RUNTIME_EXCEPTION", record.partition());

            // Catch-all
            default -> new TopicPartition(record.topic() + ".UNKNOWN_EXCEPTION", record.partition());
        };
    }
}
