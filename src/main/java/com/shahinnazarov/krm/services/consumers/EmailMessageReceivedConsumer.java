package com.shahinnazarov.krm.services.consumers;

import static com.shahinnazarov.krm.utils.Constants.BEAN_EMAIL_MESSAGE_RECEIVED_CONSUMER;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service(BEAN_EMAIL_MESSAGE_RECEIVED_CONSUMER)
public class EmailMessageReceivedConsumer implements AcknowledgingMessageListener<String, String> {
    @Override
    public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        log.info("KEY: {}, VALUE: {} at {} received.", data.key(), data.value(), data.timestamp());
        acknowledgment.acknowledge();
    }
}
