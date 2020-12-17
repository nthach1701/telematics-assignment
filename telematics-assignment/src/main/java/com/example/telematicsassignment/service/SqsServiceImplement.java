package com.example.telematicsassignment.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.aws.messaging.config.QueueMessageHandlerFactory;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.example.telematicsassignment.repository.UserInfoRepository;

@Service
public class SqsServiceImplement implements SqsService {

    private static final Logger logger = LoggerFactory.getLogger(SqsServiceImplement.class);

    @Autowired
    private AmazonClientService amazonClientService;

    @Autowired
    private UserInfoRepository userRepository;

    @Bean
    public QueueMessageHandlerFactory queueMessageHandlerFactory() {
        QueueMessageHandlerFactory factory = new QueueMessageHandlerFactory();
        MappingJackson2MessageConverter messageConverter = new MappingJackson2MessageConverter();

        // set strict content type match to false
        messageConverter.setStrictContentTypeMatch(false);
        factory.setArgumentResolvers(Collections
                .<HandlerMethodArgumentResolver>singletonList(new PayloadArgumentResolver(messageConverter)));
        return factory;
    }

    @Override
    public List<Item> readCsvFile(InputStream inputStream) throws IOException {
        BufferedReader fileReader = new BufferedReader(new InputStreamReader(inputStream));

        CSVParser csvParser = null;

        List<Item> userInfos = new ArrayList<>();

        csvParser = new CSVParser(fileReader,
                CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
        Iterable<CSVRecord> csvRecords = csvParser.getRecords();
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");

        for (CSVRecord csvRecord : csvRecords) {
            String presenDate = formatter.format(new Date());
            Item item = new Item();
            item.withPrimaryKey("id", csvRecord.get("id"));
            item.withString("first", csvRecord.get("first"));
            item.withString("last", csvRecord.get("last"));
            item.withString("dob", csvRecord.get("dob"));
            item.withString("processed_time", presenDate);

            userInfos.add(item);
        }

        fileReader.close();
        csvParser.close();

        return userInfos;
    }

    @Override
    public Map<String, List<WriteRequest>> writeMultipleItemsBatchWrite(List<Item> userInfos) {
        return userRepository.writeMultipleItemsBatchWrite(userInfos);
    }

    @Override
    public String getDestinationKeyToMove(String sourceKey, String destination) {
        StringBuilder stringBuilder = new StringBuilder(sourceKey);
        return stringBuilder.replace(0, 4, destination).toString();
    }

    @Override
    public void moveObjectFromDataToProcessed(String sourceBucketName, String sourceKey) {
        String destinationKey = getDestinationKeyToMove(sourceKey, "processed");
        CopyObjectResult resultMoveObject = amazonClientService.copyObject(sourceBucketName, sourceKey, destinationKey);
        if (resultMoveObject.getETag() != null) {
            amazonClientService.deleteObject(sourceBucketName, sourceKey);
            logger.info("move successs!!");
        }
    }

    @Override
    public void moveObjectFromDataToError(String sourceBucketName, String sourceKey) {
        String destinationKey = getDestinationKeyToMove(sourceKey, "error");
        CopyObjectResult resultMoveObject = amazonClientService.copyObject(sourceBucketName, sourceKey, destinationKey);
        if (resultMoveObject.getETag() != null) {
            amazonClientService.deleteObject(sourceBucketName, sourceKey);
        }
    }

    @Override
    public void processFile(S3Object objecToProcess, String bucketName, String objectKey) throws IOException {
        List<Item> result = readCsvFile(objecToProcess.getObjectContent());

        writeMultipleItemsBatchWrite(result);

        moveObjectFromDataToProcessed(bucketName, objectKey);
    }

    @SqsListener(value = { "http://localhost:4566/000000000000/input-file-q" })
    @Override
    public void receiveMessage(S3EventNotification s3EventNotificationRecord) {
        S3EventNotification.S3Entity s3Entity = s3EventNotificationRecord.getRecords().get(0).getS3();
        String objectKeyFromMessage = s3Entity.getObject().getKey();
        String eventFromMessage = s3EventNotificationRecord.getRecords().get(0).getEventName();
        String message = new StringBuilder().append(objectKeyFromMessage).append(eventFromMessage).toString();
        logger.info("Message Received using SQS Listner " + message);

        // Filter message name
        if (s3EventNotificationRecord.getRecords().get(0).getEventName().equals("ObjectCreated:Put")) {
            String bucketName = s3EventNotificationRecord.getRecords().get(0).getS3().getBucket().getName();
            String objectKey = s3Entity.getObject().getKey();
            S3Object object = amazonClientService.getObject(bucketName, objectKey);

            int timesToWriteWhenHasError = 10;

            try {
                processFile(object, bucketName, objectKey);
            } catch (Exception e) {
                do {
                    try {
                        processFile(object, bucketName, objectKey);
                    } catch (Exception exceptionInLoop) {
                        timesToWriteWhenHasError--;
                        String errorMessage = exceptionInLoop.getMessage();
                        logger.error("error in loop " + errorMessage);
                        continue;
                    }
                } while (timesToWriteWhenHasError > 0);
                moveObjectFromDataToError(bucketName, objectKey);
            }
        }
    }

}
