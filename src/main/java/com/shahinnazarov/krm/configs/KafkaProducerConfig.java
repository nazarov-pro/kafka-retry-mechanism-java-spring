package com.shahinnazarov.krm.configs;

import com.shahinnazarov.krm.container.models.KafkaConfigModel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {
    private final KafkaConfigModel kafkaConfigModel;
    private final ResourceLoader resourceLoader;

    public Map<String, Object> producerConfigsForStringMessage() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigModel.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.putAll(kafkaConfigModel.getSslProperties(resourceLoader));
        return props;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() throws IOException {
        DefaultKafkaProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<>(producerConfigsForStringMessage());
        return new KafkaTemplate<>(producerFactory);
    }
}
