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

/**
 * Utility class for determining dead letter queue (DLQ) routing based on exception types.
 * <p>
 * This utility provides intelligent DLQ routing by analyzing the exception that caused
 * message processing to fail and determining the appropriate dead letter topic based on
 * the exception type. Different exception types are routed to different DLQ topics
 * to enable targeted handling and analysis of failures.
 * <p>
 * The utility distinguishes between retryable exceptions (database locks, timeouts, etc.)
 * and non-retryable exceptions (validation errors, null pointers, etc.) by routing them
 * to appropriately named dead letter topics.
 * <p>
 * DLQ topic naming convention: {@code {originalTopic}.{EXCEPTION_TYPE}}
 *
 * @author Kush Parsaniya
 * @since 0.0.1
 * @see org.springframework.kafka.listener.DeadLetterPublishingRecoverer
 */
public class RecovererUtil {

    /**
     * Private constructor to prevent instantiation of this utility class.
     * This class contains only static methods and should not be instantiated.
     */
    private RecovererUtil() {
        // Utility class - no instantiation
    }

    /**
     * Determines the appropriate dead letter queue topic partition based on the exception type.
     * <p>
     * This method analyzes the exception that caused message processing to fail and routes
     * the message to an appropriate dead letter topic based on the exception type. The method
     * first unwraps any wrapper exceptions to get the root cause, then applies exception-specific
     * routing logic.
     * <p>
     * Exception routing categories:
     * <ul>
     *   <li><strong>Database Lock Issues:</strong> {@code .RETRY_DB_LOCK}, {@code .RETRY_DB_PESSIMISTIC_LOCK}</li>
     *   <li><strong>Concurrency Issues:</strong> {@code .RETRY_CONCURRENCY}</li>
     *   <li><strong>Database Timeouts:</strong> {@code .RETRY_DB_TRANSIENT}, {@code .RETRY_DB_TIMEOUT}</li>
     *   <li><strong>Kafka Issues:</strong> {@code .RETRY_KAFKA}</li>
     *   <li><strong>Validation Errors:</strong> {@code .ILLEGAL_ARGUMENT}</li>
     *   <li><strong>Null Pointer Issues:</strong> {@code .NULL_POINTER}</li>
     *   <li><strong>General Runtime Errors:</strong> {@code .RUNTIME_EXCEPTION}</li>
     *   <li><strong>Unknown Errors:</strong> {@code .UNKNOWN_EXCEPTION}</li>
     * </ul>
     *
     * @param record the Kafka consumer record that failed processing
     * @param ex     the exception that caused the failure
     * @return the target topic partition for the dead letter queue
     * @see #unwrapRootCause(Throwable)
     */
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

    /**
     * Unwraps nested exceptions to find the most meaningful root cause for DLQ routing.
     * <p>
     * This method handles the common pattern of exceptions being wrapped in multiple layers
     * of container exceptions (like {@link ListenerExecutionFailedException} or {@link KafkaException}).
     * It unwraps these container exceptions to reveal the actual business logic exception
     * that should be used for routing decisions.
     * <p>
     * The unwrapping process:
     * <ol>
     *   <li>First, unwrap known wrapper exceptions like {@code ListenerExecutionFailedException}</li>
     *   <li>Then, find the deepest cause in the exception chain</li>
     *   <li>Return the most meaningful exception for routing purposes</li>
     * </ol>
     *
     * @param ex the exception to unwrap (may be null)
     * @return the unwrapped root cause exception, or the original exception if no cause is found
     */
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
