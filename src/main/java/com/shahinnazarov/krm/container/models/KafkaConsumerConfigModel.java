package com.shahinnazarov.krm.container.models;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class KafkaConsumerConfigModel {
    private final String groupId;
    private final String retryGroupId;
    private final String enableAutoCommit;
    private final String autoOffsetReset;
    private final Integer maxPollRecords;
    private final Integer maxPollInterval;
}
