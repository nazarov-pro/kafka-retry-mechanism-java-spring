package com.shahinnazarov.krm.configs;

import static com.shahinnazarov.krm.utils.Constants.BEAN_CONTAINER_FACTORY;
import static com.shahinnazarov.krm.utils.Constants.BEAN_RETRY_CONSUMER;

import com.shahinnazarov.krm.services.consumers.RetryConsumer;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

@Configuration
@RequiredArgsConstructor
public class RetryContainerConfig {
    private final KafkaConsumerConfig consumerConfig;
    @Qualifier(BEAN_RETRY_CONSUMER)
    private final RetryConsumer retryConsumer;
    @Qualifier(BEAN_CONTAINER_FACTORY)
    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;

    @PostConstruct
    public void registerBeans() {
        consumerConfig.registerRetryContainers(retryConsumer, containerFactory);
    }
}
