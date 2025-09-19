package dev.kush.springkafkacommons;

import org.springframework.boot.context.properties.ConfigurationProperties;

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
