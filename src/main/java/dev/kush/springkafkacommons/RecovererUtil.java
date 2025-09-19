package dev.kush.springkafkacommons;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

import java.util.Objects;

public class RecovererUtil {

    public static TopicPartition getDlqTopicPartition(ConsumerRecord<?, ?> record, Exception ex) {
        // 1. unwrap to deepest cause that's helpful
        Throwable root = unwrapRootCause(ex);

        // 2. now use switch expression on the root cause
        return switch (root) {
            // retryable exceptions
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

    @SuppressWarnings("ConstantConditions")
    private static Throwable unwrapRootCause(Throwable ex) {
        if (ex == null) return null;

        // If ListenerExecutionFailedException or other wrapper, dig into its cause(s)
        Throwable current = ex;
        while (current.getCause() != null
                && (current instanceof ListenerExecutionFailedException
                || current instanceof KafkaException // wrapper check
                || current.getClass().getName().endsWith("WrapperException") // defensive
        )) {
            current = current.getCause();
        }

        // get the deepest non-null cause if present
        Throwable root = current;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return Objects.requireNonNullElse(root, ex);
    }
}
