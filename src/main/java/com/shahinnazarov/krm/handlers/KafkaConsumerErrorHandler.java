package com.shahinnazarov.krm.handlers;

import static com.shahinnazarov.krm.services.consumers.RetryConsumer.RETRY_DURATION;
import static com.shahinnazarov.krm.utils.Constants.BEAN_RETRY_TOPICS;
import static com.shahinnazarov.krm.utils.Constants.DLQ_TOPIC_SUFFIX;
import static com.shahinnazarov.krm.utils.Constants.INITIAL_TIMESTAMP_KEY;
import static com.shahinnazarov.krm.utils.Constants.TOPIC_KEYS_SEPARATOR;

import com.shahinnazarov.krm.container.models.KafkaConfigModel;
import com.shahinnazarov.krm.container.models.RetryTopicModel;
import com.shahinnazarov.krm.utils.TimeUtils;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerErrorHandler implements ConsumerAwareErrorHandler {
    @Qualifier(BEAN_RETRY_TOPICS)
    private final Map<String, RetryTopicModel> retryTopics;
    private final KafkaConfigModel kafkaConfigModel;
    private final TimeUtils timeUtils;
    private final KafkaTemplate<String, String> kafkaTemplate;

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
        data.headers().add(INITIAL_TIMESTAMP_KEY, String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8));
    }

    private void forwardToTopic(ConsumerRecord<?, ?> data) {
        final var topicName = data.topic();
        String nextRetryTopic;
        Integer nextPeriod;
        if (retryTopics.containsKey(topicName)) {
            final var retryTopicModel = retryTopics.get(topicName);
            if (retryTopicModel.isTheLastRetryTopic()) {
                publishToDlqTopic(retryTopicModel.getTopicName().concat(retryTopicModel.getDlqSuffix()), data);
                return;
            }
            nextPeriod = retryTopicModel.getNextMinute();
            nextRetryTopic = retryTopicModel.getNextTopicName();
        } else if (kafkaConfigModel.getTopics().containsKey(topicName)) {
            final var kafkaTopic = kafkaConfigModel.getTopics().get(topicName);
            if (kafkaTopic.getRetryMinutes().isEmpty()) {
                publishToDlqTopic(kafkaTopic.getTopic().concat(kafkaTopic.getDlqSuffix()), data);
                return;
            }
            nextPeriod = getHeader(RETRY_DURATION, data)
                    .map(Integer::parseInt)
                    .map(minute -> kafkaTopic.getRetryMinutes().stream().filter(e -> e > minute)
                            .findFirst().orElseThrow())
                    .orElseGet(() -> kafkaTopic.getRetryMinutes().stream().findFirst().orElseThrow());
            nextRetryTopic = RetryTopicModel.convertTopicName(
                    topicName,
                    kafkaTopic.getRetryTopicSuffix(),
                    nextPeriod
            );
        } else {
            log.warn("Topic {} was not expected.", topicName);
            publishToDlqTopic(data.topic().concat(TOPIC_KEYS_SEPARATOR).concat(DLQ_TOPIC_SUFFIX), data);
            return;
        }
        publishToRetryTopic(nextRetryTopic, nextPeriod.toString(), data);
    }

    @SneakyThrows
    private void publishToRetryTopic(String newRetryTopic, String nextPeriod, ConsumerRecord<?, ?> data) {
        final var pr = new ProducerRecord<>(
                newRetryTopic, Objects.toString(data.key()), Objects.toString(data.value())
        );
        pr.headers().add(RETRY_DURATION, nextPeriod.getBytes());
        updateTimestampHeader(pr, timeUtils.epochMillis());
        kafkaTemplate.send(pr).get();
    }

    @SneakyThrows
    private void publishToDlqTopic(String dlqTopic, ConsumerRecord<?, ?> data) {
        final var pr = new ProducerRecord<>(
                dlqTopic, Objects.toString(data.key()), Objects.toString(data.value())
        );
        updateTimestampHeader(pr, timeUtils.epochMillis());
        kafkaTemplate.send(pr).get();
    }

    @Override
    public boolean isAckAfterHandle() {
        return true;
    }
}
