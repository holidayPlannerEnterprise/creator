package com.arpajit.holidayplanner.creator;

import org.slf4j.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.arpajit.holidayplanner.dto.*;

@Component
public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = "holidayplanner-creator", groupId = "holidayplanner-controller")
    public void consumeRequest(ControllerProducer contrllerProducer) {
        logger.info("Received Kafka response: {}", contrllerProducer);
        if ("GET_ALL_HOLIDAYS".equals(contrllerProducer.getRequestType())) {
            Object payload = contrllerProducer.getPayload();
            logger.info("Payload received for holidays: {}", payload);
        }
    }
}
