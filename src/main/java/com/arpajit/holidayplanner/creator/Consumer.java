package com.arpajit.holidayplanner.creator;

import org.slf4j.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import com.arpajit.holidayplanner.dto.*;

@Component
public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = "holidayplanner-creator", groupId = "holidayplanner-controller")
    public void consumeRequest(@RequestBody ControllerConsumer controllerConsumer) {
        logger.info("Received Kafka response: {}", controllerConsumer);
        if ("GET_ALL_HOLIDAYS".equals(controllerConsumer.getRequestType())) {
            Object payload = controllerConsumer.getPayload();
            logger.info("Payload received for holidays: {}", payload);
        }
    }
}
