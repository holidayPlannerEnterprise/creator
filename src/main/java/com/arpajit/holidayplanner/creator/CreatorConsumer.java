package com.arpajit.holidayplanner.creator;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.arpajit.holidayplanner.dto.*;

@Component
public class CreatorConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CreatorConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private CreatorDataServComm dataService;

    @KafkaListener(topics = "holidayplanner-creator", groupId = "holidayplanner-controller")
    public void consumedPayload(String payload, Acknowledgment act) throws Exception {
        logger.info("Received Kafka response: {}", payload);
        act.acknowledge();
        ConsumeMessage message = objectMapper.readValue(payload, ConsumeMessage.class);
        dataService.updateAudit(message.getTraceId(), "CREATOR_RECEIVED", null);
        logger.info("Requested for {}", message.getRequestType());
        switch (message.getRequestType()) {
            case "GET_ALL_HOLIDAYS":
                logger.info("Triggering GET_ALL_HOLIDAYS service");
                break;
            default:
                logger.warn("{} does not match any request type", message.getRequestType());
                dataService.updateAudit(message.getTraceId(),
                                        "CREATOR_RECEIVED",
                                        "Request type: "+message.getRequestType()+" not matched in Consumer");
                break;
        }
    }
}
