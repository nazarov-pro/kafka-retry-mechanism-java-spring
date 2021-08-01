package com.shahinnazarov.krm.container.models;

import lombok.Data;

@Data
public class KafkaConsumerConfigModel {
    private String groupId;
    private String retryGroupId;
    private String enableAutoCommit;
    private String autoOffsetReset;
    private Integer maxPollRecords;
    private Integer maxPollInterval;
}
