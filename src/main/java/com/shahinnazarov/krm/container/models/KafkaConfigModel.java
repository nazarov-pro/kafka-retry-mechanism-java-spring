package com.shahinnazarov.krm.container.models;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

@Component
@Getter
@ConfigurationProperties(prefix = "app.kafka")
@Slf4j
@RequiredArgsConstructor
public class KafkaConfigModel {
    private final String bootstrapServers;
    private final KafkaSslConfigModel ssl;
    private final KafkaConsumerConfigModel consumer;
    private final KafkaProducerConfigModel producer;
    private final Map<String, KafkaTopicConfigModel> topics;

    private Map<String, RetryTopicModel> retryEnabledTopics;

    @PostConstruct
    public void init() {
        retryEnabledTopics = topics.entrySet().stream()
                .flatMap(topic -> {
                            Integer[] minutes = topic.getValue().getRetryMinutes().toArray(new Integer[] {});
                            return IntStream.range(0, minutes.length).mapToObj(
                                    index -> new RetryTopicModel(
                                            topic.getKey(),
                                            topic.getValue().getRetryTopicSuffix(),
                                            minutes[index],
                                            topic.getValue().getDlqSuffix(),
                                            index + 1 == minutes.length ? null : minutes[index + 1]
                                    )
                            );
                        }
                ).collect(Collectors.toUnmodifiableMap(RetryTopicModel::getTopicName, Function.identity()));
    }

    public Map<String, Object> getSslProperties(ResourceLoader resourceLoader) throws IOException {
        if (ssl != null && ssl.getEnabled()) {
            String sslTrustedStorePath = resourceLoader.getResource(ssl.getTrustStoreLocation())
                    .getFile().getAbsolutePath();
            return Map.of(
                    "security.protocol", ssl.getProtocol(),
                    "ssl.truststore.location", sslTrustedStorePath,
                    "ssl.truststore.password", ssl.getTrustStorePassword()
            );
        }
        return Collections.emptyMap();
    }
}
