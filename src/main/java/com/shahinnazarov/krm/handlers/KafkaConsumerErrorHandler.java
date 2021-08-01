package com.shahinnazarov.krm.handlers;

import static com.shahinnazarov.krm.services.consumers.RetryConsumer.RETRY_DURATION;

import com.shahinnazarov.krm.utils.TimeUtils;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerErrorHandler implements ConsumerAwareErrorHandler {

    public static final List<Integer> PERIODS = List.of(0, 1, 5, 15);
    public static final String INITIAL_TIMESTAMP_KEY = "initial_timestamp";
    public static final String RETRY_TOPIC_KEY = "_RETRY_";
    private final TimeUtils timeUtils;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public static Integer extractCurrentPeriod(ConsumerRecord<?, ?> data) {
        return getHeader(RETRY_DURATION, data)
                .map(Integer::valueOf)
                .orElse(0);
    }

    public static String extractBaseTopic(String topic) {
        return topic.substring(0, topic.length() - RETRY_TOPIC_KEY.length() - 2);
    }

    public static Optional<String> getHeader(String key, ConsumerRecord<?, ?> consumerRecord) {
        return Optional.ofNullable(consumerRecord.headers().lastHeader(key))
                .map(Header::value)
                .map(String::new);
    }

    @Override
    public void handle(Exception thrownException, ConsumerRecord<?, ?> data, Consumer<?, ?> consumer) {
        if (data == null) {
            log.error("Consumer record data is null, exception:", thrownException);
            consumer.commitSync();
            return;
        }
        log.error("{} for record id {}", thrownException, data.key());
        forwardToTopic(data);
        consumer.commitSync();
    }

    private void updateTimestampHeader(ProducerRecord<?, ?> data, long timestamp) {
        data.headers()
                .add(INITIAL_TIMESTAMP_KEY, String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8));
    }

    private void forwardToTopic(ConsumerRecord<?, ?> data) {
        var currentPeriod = extractCurrentPeriod(data);

        if (isLastPeriod(currentPeriod)) {
            publishToDlq(data);
            return;
        }

        String nextPeriod = findNextPeriod(currentPeriod);

        String newRetryTopic = getNextPeriodTopic(data.topic(), nextPeriod);
        publishToRetryTopic(newRetryTopic, nextPeriod, data);
    }

    @SneakyThrows
    private void publishToRetryTopic(String newRetryTopic, String nextPeriod, ConsumerRecord<?, ?> data) {
        final var pr =
                new ProducerRecord<>(newRetryTopic, Objects.toString(data.key()), Objects.toString(data.value()));
        pr.headers().add(RETRY_DURATION, nextPeriod.getBytes());
        updateTimestampHeader(pr, timeUtils.epochMillis());
        kafkaTemplate.send(pr).get();
    }

    private String getNextPeriodTopic(String topic, String nextPeriod) {
        return topic.contains(RETRY_TOPIC_KEY)
                ? String.format("%s%s%s", extractBaseTopic(topic), RETRY_TOPIC_KEY, nextPeriod)
                : String.format("%s%s%s", topic, RETRY_TOPIC_KEY, nextPeriod);
    }

    private String findNextPeriod(Integer currentPeriod) {
        return String.format("%02d", PERIODS.get(PERIODS.indexOf(currentPeriod) + 1));
    }

    private void publishToDlq(ConsumerRecord<?, ?> data) {
        var dlqTopic = (!data.topic().contains(RETRY_TOPIC_KEY)
                ? data.topic()
                : extractBaseTopic(data.topic()))
                .concat(".DLQ");
        publishToRetryTopic(dlqTopic, "99", data);
    }

    private boolean isLastPeriod(Integer currentPeriod) {
        return currentPeriod.equals(PERIODS.get(PERIODS.size() - 1));
    }

    @Override
    public boolean isAckAfterHandle() {
        return true;
    }
}
