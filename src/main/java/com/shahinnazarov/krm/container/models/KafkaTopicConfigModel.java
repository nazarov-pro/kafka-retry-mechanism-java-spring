package com.shahinnazarov.krm.container.models;

import static com.shahinnazarov.krm.utils.Constants.DLQ_TOPIC_SUFFIX;
import static com.shahinnazarov.krm.utils.Constants.RETRY_TOPIC_SUFFIX;
import static com.shahinnazarov.krm.utils.Constants.TOPIC_KEYS_SEPARATOR;

import com.shahinnazarov.krm.container.enums.TopicCreationModes;
import java.util.Collection;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class KafkaTopicConfigModel {
    private final String topic;
    private final Integer partitions;
    private final Integer replicationFactor;
    private final Integer concurrency;
    private final TopicCreationModes creationMode;
    private final Collection<Integer> retryMinutes;
    private final String retrySuffix;
    private final String dlqSuffix;

    private static SortedSet<Integer> retryMinutesOrdered;

    //_{PREFIX}_{MINUTES} or _{MINUTES}
    public String getRetryTopicSuffix() {
        return TOPIC_KEYS_SEPARATOR.concat(Optional.ofNullable(retrySuffix).orElse(RETRY_TOPIC_SUFFIX))
                .concat(TOPIC_KEYS_SEPARATOR);
    }

    public String getDlqSuffix() {
        return TOPIC_KEYS_SEPARATOR.concat(Optional.ofNullable(dlqSuffix).orElse(DLQ_TOPIC_SUFFIX));
    }

    public Collection<Integer> getRetryMinutes() {
        if (retryMinutesOrdered == null) {
            retryMinutesOrdered = Optional.ofNullable(retryMinutes)
                    .map(TreeSet::new).orElse(new TreeSet<>());
        }
        return retryMinutesOrdered;
    }
}
