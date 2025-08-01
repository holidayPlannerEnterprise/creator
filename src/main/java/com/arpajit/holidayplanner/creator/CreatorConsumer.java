package com.arpajit.holidayplanner.creator;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
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

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "holidayplanner-creator", groupId = "holidayplanner-controller")
    public void consumedPayload(String payload,
                                @Header(KafkaHeaders.REPLY_TOPIC) String replyTopic,
                                Acknowledgment act)
                                throws Exception {
        logger.info("Received Kafka response: {}", payload);
        act.acknowledge();
        ConsumeMessage message = objectMapper.readValue(payload, ConsumeMessage.class);
        dataService.updateAudit(message.getTraceId(), "CREATOR_RECEIVED", null);
        logger.info("Requested for {}", message.getRequestType());
        switch (message.getRequestType()) {
            case "GET_ALL_HOLIDAYS":
                logger.info("Triggering GET_ALL_HOLIDAYS service");
                String response = "{" + "Status" + ": " + "ok" + "}";
                dataService.updateAudit(message.getTraceId(), "CONTROLLER_SENT", null);
                kafkaTemplate.send(replyTopic, response);
                break;
            default:
                logger.warn("{} does not match any request type", message.getRequestType());
                dataService.updateAudit(message.getTraceId(),
                                        "FAILED_AT_CREATOR",
                                        "Request type: "+message.getRequestType()+" not matched in Consumer");
                break;
        }
    }
}
