package com.shahinnazarov.krm.configs;

import static com.shahinnazarov.krm.utils.Constants.BEAN_CONTAINER_FACTORY;
import static com.shahinnazarov.krm.utils.Constants.BEAN_RETRY_CONSUMER;

import com.shahinnazarov.krm.container.models.KafkaConfigModel;
import com.shahinnazarov.krm.services.consumers.RetryConsumer;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Configuration
@RequiredArgsConstructor
public class RetryContainerConfig {
    private final KafkaConfigModel kafkaConfigModel;
    @Qualifier(BEAN_RETRY_CONSUMER)
    private final RetryConsumer retryConsumer;
    @Qualifier(BEAN_CONTAINER_FACTORY)
    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;
    private final ConfigurableApplicationContext ctx;

    @PostConstruct
    public void registerBeans() {
        kafkaConfigModel.getRetryEnabledTopics().keySet().forEach(
                topic -> {
                    ConcurrentMessageListenerContainer<String, String> container =
                            containerFactory.createContainer(topic);
                    container.setAutoStartup(true);
                    final var beanName = "RETRY_BEAN_".concat(topic);
                    container.setBeanName(beanName);
                    container.getContainerProperties().setMissingTopicsFatal(false);
                    container.setupMessageListener(retryConsumer);

                    ctx.getBeanFactory().registerSingleton(beanName, container);
                }
        );
    }
}
