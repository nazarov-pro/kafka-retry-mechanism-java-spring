package com.shahinnazarov.krm.configs;

import static com.shahinnazarov.krm.utils.Constants.BEAN_CONTAINER_FACTORY;
import static com.shahinnazarov.krm.utils.Constants.BEAN_EMAIL_MESSAGE_RECEIVED_CONSUMER;
import static com.shahinnazarov.krm.utils.Constants.BEAN_EMAIL_MESSAGE_RECEIVED_CONTAINER;
import static org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE;

import com.fasterxml.jackson.core.JsonParseException;
import com.shahinnazarov.krm.container.models.KafkaConfigModel;
import com.shahinnazarov.krm.container.models.KafkaTopicConfigModel;
import com.shahinnazarov.krm.handlers.KafkaConsumerErrorHandler;
import com.shahinnazarov.krm.services.consumers.EmailMessageReceivedConsumer;
import com.shahinnazarov.krm.services.consumers.RetryConsumer;
import com.shahinnazarov.krm.utils.Constants;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    public static final long BACK_OFF_PERIOD = 250L;
    public static final int MAX_ATTEMPTS = 2;
    public static final String RETRY_TOPIC_PATTERN = "%s%s%s";
    private final KafkaConfigModel kafkaConfigModel;
    private final KafkaTopicConfig kafkaTopicConfig;
    private final ResourceLoader resourceLoader;
    private final KafkaConsumerErrorHandler errorHandler;
    private final ConfigurableApplicationContext ctx;

    public Map<String, Object> consumerStringStringProperties() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigModel.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfigModel.getConsumer().getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfigModel.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConfigModel.getConsumer().getMaxPollRecords());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaConfigModel.getConsumer().getEnableAutoCommit());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.putAll(kafkaConfigModel.getSslProperties(resourceLoader));
        return props;
    }

    @Bean(BEAN_CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainer()
            throws IOException {
        Map<String, Object> properties = consumerStringStringProperties();
        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties));
        listenerContainerFactory.setRetryTemplate(retryTemplate());
        listenerContainerFactory.setErrorHandler(errorHandler);
        listenerContainerFactory.getContainerProperties().setLogContainerConfig(false);
        listenerContainerFactory.setAutoStartup(true);
        Optional.ofNullable(kafkaConfigModel.getConsumer().getEnableAutoCommit()).filter(Predicate.isEqual("false"))
                .ifPresent(tmp -> listenerContainerFactory.getContainerProperties().setAckMode(MANUAL_IMMEDIATE));
        return listenerContainerFactory;
    }

    @Bean(BEAN_EMAIL_MESSAGE_RECEIVED_CONTAINER)
    public ConcurrentMessageListenerContainer<String, String> userEventContainer(
            @Qualifier(BEAN_EMAIL_MESSAGE_RECEIVED_CONSUMER) EmailMessageReceivedConsumer emailMessageReceivedConsumer,
            @Qualifier(BEAN_CONTAINER_FACTORY) ConcurrentKafkaListenerContainerFactory<String, String> containerFactory
    ) {
        KafkaTopicConfigModel kafkaTopicConfigModel =
                kafkaTopicConfig.getTopicName(Constants.KEY_EMAIL_MESSAGE_RECEIVED_TOPIC).orElseThrow();
        ConcurrentMessageListenerContainer<String, String> container = containerFactory
                .createContainer(kafkaTopicConfigModel.getTopic());
        container.setAutoStartup(true);
        container.setBeanName(BEAN_EMAIL_MESSAGE_RECEIVED_CONTAINER);
        container.setConcurrency(kafkaTopicConfigModel.getConcurrency());
        container.setupMessageListener(emailMessageReceivedConsumer);
        return container;
    }


    public void registerRetryContainers(RetryConsumer retryConsumer,
                                        ConcurrentKafkaListenerContainerFactory<String, String> containerFactory
    ) {
        registerRetryContainers(retryConsumer, containerFactory, getRetryableTopics("01"));
        registerRetryContainers(retryConsumer, containerFactory, getRetryableTopics("05"));
        registerRetryContainers(retryConsumer, containerFactory, getRetryableTopics("15"));
    }

    private void registerRetryContainers(
            RetryConsumer retryConsumer,
            ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
            String[] topics) {
        for (String topic : topics) {
            ConcurrentMessageListenerContainer<String, String> container =
                    containerFactory.createContainer(topic);
            container.setAutoStartup(true);
            final var beanName = "RETRY_BEAN_".concat(topic);
            container.setBeanName(beanName);
            container.getContainerProperties().setMissingTopicsFatal(false);
            container.setupMessageListener(retryConsumer);

            ctx.getBeanFactory().registerSingleton(beanName, container);
        }
    }

    private RetryTemplate retryTemplate() {
        final var retryTemplate = new RetryTemplate();

        final var backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(BACK_OFF_PERIOD);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        final Map<Class<? extends Throwable>, Boolean> errorPolicy = Map.of(JsonParseException.class, false);
        final var retryPolicy = new SimpleRetryPolicy(MAX_ATTEMPTS, errorPolicy, true, true);
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }

    private String[] getRetryableTopics(String minutes) {
        return kafkaConfigModel.getTopics().values().stream().filter(KafkaTopicConfigModel::getRetryEnabled)
                .map(KafkaTopicConfigModel::getTopic).map(
                        topicName -> String
                                .format(RETRY_TOPIC_PATTERN, topicName, KafkaConsumerErrorHandler.RETRY_TOPIC_KEY,
                                        minutes)
                ).toArray(String[]::new);
    }

}
