package com.arpajit.holidayplanner.creator;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.arpajit.holidayplanner.creator.service.CreatorService;
import com.arpajit.holidayplanner.creator.dto.KafkaMessage;

@Component
public class CreatorConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CreatorConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private CreatorDataServComm dataService;

    @Autowired
    private CreatorService creatorService;

    @Autowired
    private CreatorProducer creatorProducer;

    @KafkaListener(topics = "holidayplanner-creator", groupId = "holidayplanner")
    public void consumedPayload(String message,
                                @Header(KafkaHeaders.CORRELATION_ID) byte[] correlationId,
                                Acknowledgment act)
                                throws Exception {
        logger.info("Received Kafka message:{}", message);
        act.acknowledge();

        // Parsing Message
        KafkaMessage messageDTO = objectMapper.readValue(message, KafkaMessage.class);
        messageDTO.setStatus("CREATOR_RECEIVED");
        logger.info("messageDTO traceID: {}", messageDTO.getTraceId());
        dataService.updateAudit(messageDTO.getTraceId(),
                                messageDTO.getStatus(),
                                messageDTO.getStatusResp());

        // Routing based on request type
        logger.info("Requested for {}", messageDTO.getRequestType());
        switch (messageDTO.getRequestType()) {
            case "GET_ALL_HOLIDAY_DETAILS":
                logger.info("Triggering GET_ALL_HOLIDAY_DETAILS service");
                messageDTO.setPayload(creatorService.getAllHolidayDetails());
                messageDTO.setStatus("DISPATCHER_SENT");
                creatorProducer.dropToDispatcher(correlationId, messageDTO);
                break;
            default:
                logger.warn("{} does not match any request type", messageDTO.getRequestType());
                messageDTO.setStatus("FAILED_AT_CREATOR");
                messageDTO.setStatusResp("Request type: "+messageDTO.getRequestType()+" not matched in Creator");
                dataService.updateAudit(messageDTO.getTraceId(),
                                        messageDTO.getStatus(),
                                        messageDTO.getStatusResp());
                break;
        }
    }
}
