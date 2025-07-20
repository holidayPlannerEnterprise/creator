package com.arpajit.holidayplanner.creator;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.arpajit.holidayplanner.dto.*;

@Component
public class HolidaysConsumer {
    private static final Logger logger = LoggerFactory.getLogger(HolidaysConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "holidayplanner-creator", groupId = "holidayplanner-controller")
    public void consumedPayload(String payload, Acknowledgment act) throws Exception {
        logger.info("Received Kafka response: {}", payload);
        act.acknowledge();
        ConsumeMessage message = objectMapper.readValue(payload, ConsumeMessage.class);
        switch (message.getRequestType()) {
            case "GET_ALL_HOLIDAYS":
                logger.info("Requested for {}", message.getRequestType());
                break;
            default:
                logger.warn("{} does not match any request type", message.getRequestType());
                break;
        }
    }
}
