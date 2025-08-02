package com.arpajit.holidayplanner.creator;

import java.net.URI;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class CreatorDataServComm {
    private static final Logger logger = LoggerFactory.getLogger(CreatorDataServComm.class);
    private static final String dsDomain = "http://localhost:8000/dataservice";

    @Autowired
    private RestTemplate restTemplate;

    public String addAudit(String payload) {
        String dsUrl = dsDomain + "/add-audit";
        logger.info("Calling URL: {}", dsUrl);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(dsUrl, request, String.class);
        logger.info("{} : gave Status : {}", dsUrl, response.getStatusCode());
        logger.info("Received:\n{}\n",response.getBody());
        return response.getBody();
    }

    public String updateAudit(String traceId, String status, String statusResp) {
        String payload = "{\"traceId\":\""+traceId+
                            "\",\"status\":\""+status+
                            "\",\"statusResp\":\""+statusResp+"\"}";
        logger.info("payload to update: {}", payload);
        String dsUrl = dsDomain + "/update-audit";
        logger.info("Calling URL: {}", dsUrl);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(dsUrl, request, String.class);
        logger.info("{} : gave Status : {}", dsUrl, response.getStatusCode());
        logger.info("Received:\n{}\n",response.getBody());
        return response.getBody();
    }

    public String allHolidayDetails() throws Exception {
        String dsUrl = dsDomain + "/all-holiday-details";
        logger.info("Calling URL: {}", dsUrl);
        ResponseEntity<String> response = restTemplate.getForEntity(new URI(dsUrl), String.class);
        logger.info("{} : gave Status : {}", dsUrl, response.getStatusCode());
        logger.info("Received:\n{}\n",response.getBody());
        return response.getBody();
    }
}
