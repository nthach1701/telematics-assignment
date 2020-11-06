package com.example.telematicsassignment;


import org.springframework.boot.SpringApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
@EnableAutoConfiguration(exclude = {org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration.class})
public class TelematicsAssignmentApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(TelematicsAssignmentApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(TelematicsAssignmentApplication.class, args);
        LOGGER.info("Springboot with amazonsqs application started successfully.");
    }
}
