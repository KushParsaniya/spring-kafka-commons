package dev.kush.springkafkacommons;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration properties for Kafka commons library.
 * <p>
 * This record defines all the configurable properties for Kafka producer and consumer
 * configurations with the prefix {@code kafka.commons}. These properties can be set in
 * {@code application.properties} or {@code application.yml} files.
 * <p>
 * <b>Default values:</b>
 * <ul>
 *   <li><b>bootstrapServers</b>: Defaults to value of {@code spring.kafka.bootstrap-servers} or {@code localhost:9092}</li>
 *   <li><b>clientId</b>: Defaults to value of {@code spring.application.name} or {@code default-client}</li>
 *   <li><b>groupId</b>: Defaults to {@code default-group}</li>
 *   <li><b>consumerConcurrency</b>: Defaults to {@code 3}</li>
 *   <li><b>retryIntervalMs</b>: Defaults to {@code 1000} ms</li>
 *   <li><b>retryMaxAttempts</b>: Defaults to {@code 3}</li>
 * </ul>
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
        @DefaultValue("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
        @DefaultValue("${spring.application.name:default-client}") String clientId,
        @DefaultValue("default-group") String groupId,
        @DefaultValue("3") Integer consumerConcurrency,
        @DefaultValue("1000") long retryIntervalMs,
        @DefaultValue("3") long retryMaxAttempts
) {
}