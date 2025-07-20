package com.arpajit.holidayplanner.creator;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.arpajit.holidayplanner.dto.*;
import com.arpajit.holidayplanner.data.model.*;
import com.arpajit.holidayplanner.data.repository.*;

@Component
public class HolidaysConsumer {
    private static final Logger logger = LoggerFactory.getLogger(HolidaysConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MessageAuditRepository messageAuditRepository;

    @KafkaListener(topics = "holidayplanner-creator", groupId = "holidayplanner-controller")
    public void consumedPayload(String payload, Acknowledgment act) throws Exception {
        logger.info("Received Kafka response: {}", payload);
        act.acknowledge();
        ConsumeMessage message = objectMapper.readValue(payload, ConsumeMessage.class);
        MessageAudits messageAudits = new MessageAudits();
        messageAudits.setMsgRequestType(message.getRequestType());
        messageAudits.setMsgSourceService(message.getSourceService());
        messageAudits.setMsgTimestamp(message.getTimestamp());
        messageAudits.setMsgPayload(message.getPayload());
        messageAudits.setMsgStatus("RECEIVED");
        messageAuditRepository.save(messageAudits);
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
