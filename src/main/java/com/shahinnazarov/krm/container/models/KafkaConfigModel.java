package com.shahinnazarov.krm.container.models;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

@Component
@Data
@ConfigurationProperties(prefix = "app.kafka")
@Slf4j
public class KafkaConfigModel {
    private String bootstrapServers;
    private KafkaSslConfigModel ssl;
    private KafkaConsumerConfigModel consumer;
    private KafkaProducerConfigModel producer;
    private Map<String, KafkaTopicConfigModel> topics;

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
