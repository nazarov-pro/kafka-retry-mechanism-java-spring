package com.shahinnazarov.krm.services.consumers;

import static com.shahinnazarov.krm.utils.Constants.BEAN_RETRY_CONSUMER;
import static com.shahinnazarov.krm.utils.Constants.INITIAL_TIMESTAMP_KEY;

import com.shahinnazarov.krm.container.models.KafkaConfigModel;
import com.shahinnazarov.krm.handlers.KafkaConsumerErrorHandler;
import com.shahinnazarov.krm.utils.TimeUtils;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service(BEAN_RETRY_CONSUMER)
public class RetryConsumer implements AcknowledgingMessageListener<String, String> {

    public static final String RETRY_DURATION = "RetryDuration";
    private static final long ONE_MINUTE_IN_MILLIS = 60000L;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConfigModel kafkaConfigModel;
    private final ApplicationContext applicationContext;
    private final TimeUtils timeUtils;

    @SneakyThrows
    @Override
    public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        log.info("Retry message received {} id. {} ||| {}", data.key(), data.topic(), data.offset());
        final var retryTopicModel = kafkaConfigModel.getRetryEnabledTopics().get(data.topic());

        final var container = getContainer(data.topic());
        container.pause();
        waitForTime(data, retryTopicModel.getMinute());
        container.resume();

        final var producerRecord = new ProducerRecord<>(retryTopicModel.getTopicName(), data.key(), data.value());

        producerRecord.headers().add(data.headers().lastHeader(INITIAL_TIMESTAMP_KEY));
        producerRecord.headers().add(RETRY_DURATION, String.valueOf(retryTopicModel.getMinute())
                .getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(producerRecord).get();
        acknowledgment.acknowledge();
    }

    private ConcurrentMessageListenerContainer<String, String> getContainer(String topic) {
        final var beanName = "RETRY_BEAN_".concat(topic);
        return (ConcurrentMessageListenerContainer<String, String>) applicationContext.getBean(beanName);
    }

    @SneakyThrows
    private void waitForTime(ConsumerRecord<String, String> data, Integer duration) {
        final var currentTimestamp = timeUtils.epochMillis();
        final var initialTimestamp = KafkaConsumerErrorHandler
                .getHeader(INITIAL_TIMESTAMP_KEY, data)
                .map(Long::valueOf)
                .orElse(currentTimestamp);
        final var expirationTimestamp = initialTimestamp + ONE_MINUTE_IN_MILLIS * duration;
        if (expirationTimestamp > currentTimestamp) {
            final var diff = expirationTimestamp - currentTimestamp;
            Thread.sleep(diff);
        }
    }
}
