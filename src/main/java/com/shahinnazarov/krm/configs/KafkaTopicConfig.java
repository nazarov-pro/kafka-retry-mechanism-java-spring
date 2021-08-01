package com.shahinnazarov.krm.configs;

import com.shahinnazarov.krm.container.models.KafkaConfigModel;
import com.shahinnazarov.krm.container.models.KafkaTopicConfigModel;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {
    private final KafkaConfigModel kafkaConfigModel;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigModel.getBootstrapServers());
        return new KafkaAdmin(configs);
    }

    public Optional<KafkaTopicConfigModel> getTopicName(String key) {
        return Optional.ofNullable(kafkaConfigModel.getTopics().get(key));
    }
}
