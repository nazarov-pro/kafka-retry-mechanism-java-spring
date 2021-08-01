package com.shahinnazarov.krm.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
    public static final String DEFAULT_TIMEZONE = "UTC";
    public static final String BEAN_RETRY_CONSUMER = "retry-cns";
    public static final String BEAN_EMAIL_MESSAGE_RECEIVED_CONSUMER = "email-message-received-cns";
    public static final String BEAN_EMAIL_MESSAGE_RECEIVED_CONTAINER = "email-message-received-cnt";
    public static final String BEAN_CONTAINER_FACTORY = "cnt-factory";

    public static final String KEY_EMAIL_MESSAGE_RECEIVED_TOPIC = "app.kafka.topics.email-message-received";

}
