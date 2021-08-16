package com.shahinnazarov.krm.container.models;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class RetryTopicModel {
    private final String topicName;
    private final String retryTopicSuffix;
    private final Integer minute;
    private final String dlqSuffix;
    private final Integer nextMinute;

    public String getTopicName() {
        return convertTopicName(topicName, retryTopicSuffix, minute);
    }

    public String getNextTopicName() {
        return convertTopicName(topicName, retryTopicSuffix, nextMinute);
    }

    public static String convertTopicName(
            String topicName,
            String retryTopicSuffix,
            Integer minute
    ) {
        return String.format(
                "%s%s%d",
                topicName,
                retryTopicSuffix,
                minute
        );
    }

    public boolean isTheLastRetryTopic() {
        return nextMinute == null;
    }
}
