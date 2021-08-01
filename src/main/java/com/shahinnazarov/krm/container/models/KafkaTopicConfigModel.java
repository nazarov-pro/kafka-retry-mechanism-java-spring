package com.shahinnazarov.krm.container.models;

import com.shahinnazarov.krm.container.enums.TopicCreationModes;
import lombok.Data;

@Data
public class KafkaTopicConfigModel {
    private String topic;
    private Integer partitions;
    private Integer replicationFactor;
    private Integer concurrency;
    private TopicCreationModes creationMode;
    private Boolean retryEnabled = false;

}
