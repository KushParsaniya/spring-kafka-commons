package dev.kush.springkafkacommons;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Kafka commons library.
 * <p>
 * This record defines all the configurable properties for Kafka producer and consumer
 * configurations with the prefix "kafka.commons". These properties can be set in
 * application.properties or application.yml files.
 *
 * @param bootstrapServers     comma-separated list of Kafka broker addresses (e.g., "localhost:9092")
 * @param clientId            unique identifier for this Kafka client instance
 * @param groupId             consumer group identifier for Kafka consumers
 * @param consumerConcurrency number of concurrent consumer threads for message processing
 * @param retryIntervalMs     interval in milliseconds between retry attempts for failed messages
 * @param retryMaxAttempts    maximum number of retry attempts before sending to dead letter queue
 *
 * @author Kush Parsaniya
 * @since 0.0.1
 */
@ConfigurationProperties(prefix = "kafka.commons")
public record KafkaCommonsProperties(
        String bootstrapServers,
        String clientId,
        String groupId,
        Integer consumerConcurrency,
        long retryIntervalMs,
        long retryMaxAttempts
) {
}
