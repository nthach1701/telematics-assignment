//package com.example.telematicsassignment.entity;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Date;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.commons.csv.CSVFormat;
//import org.apache.commons.csv.CSVParser;
//import org.apache.commons.csv.CSVRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.cloud.aws.messaging.config.QueueMessageHandlerFactory;
//import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
//import org.springframework.context.annotation.Bean;
//import org.springframework.messaging.converter.MappingJackson2MessageConverter;
//import org.springframework.messaging.handler.annotation.support.PayloadArgumentResolver;
//import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
//import org.springframework.stereotype.Component;
//import com.amazonaws.client.builder.AwsClientBuilder;
//import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
//import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
//import com.amazonaws.services.dynamodbv2.document.DynamoDB;
//import com.amazonaws.services.dynamodbv2.document.Item;
//import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
//import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
//import com.amazonaws.services.dynamodbv2.model.WriteRequest;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.event.S3EventNotification;
//import com.amazonaws.services.s3.model.CopyObjectRequest;
//import com.amazonaws.services.s3.model.CopyObjectResult;
//import com.amazonaws.services.s3.model.GetObjectRequest;
//import com.amazonaws.services.s3.model.S3Object;
//
//@Component
//public class Consumer {
//
//    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
//
//    // Value is populated by the region code.
//    @Value("${cloud.aws.region.static}")
//    private static String region;
//
//    // Value is populated with the aws access key.
//    @Value("${cloud.aws.credentials.access-key}")
//    private String awsAccessKey;
//
//    // Value is populated with the aws secret key
//    @Value("${cloud.aws.credentials.secret-key}")
//    private String awsSecretKey;
//
//    @Bean
//    public QueueMessageHandlerFactory queueMessageHandlerFactory() {
//        QueueMessageHandlerFactory factory = new QueueMessageHandlerFactory();
//        MappingJackson2MessageConverter messageConverter = new MappingJackson2MessageConverter();
//
//        // set strict content type match to false
//        messageConverter.setStrictContentTypeMatch(false);
//        factory.setArgumentResolvers(Collections
//                .<HandlerMethodArgumentResolver>singletonList(new PayloadArgumentResolver(messageConverter)));
//        return factory;
//    }
//
//    // generate amazons3 connection for copying and moving object in bucket
//    public static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566/", region))
//            .enablePathStyleAccess().build();
//
//    // read csv file
//    public List<Item> readCsvFile(InputStream inputStream) throws IOException {
//        BufferedReader fileReader = new BufferedReader(new InputStreamReader(inputStream));
//
//        CSVParser csvParser = null;
//
//        List<Item> userInfos = new ArrayList<>();
//
//        csvParser = new CSVParser(fileReader,
//                CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
//        Iterable<CSVRecord> csvRecords = csvParser.getRecords();
//        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
//
//        for (CSVRecord csvRecord : csvRecords) {
//            String presenDate = formatter.format(new Date());
//            Item item = new Item();
//            item.withPrimaryKey("id", csvRecord.get("id"));
//            item.withString("first", csvRecord.get("first"));
//            item.withString("last", csvRecord.get("last"));
//            item.withString("dob", csvRecord.get("dob"));
//            item.withString("processed_time", presenDate);
//
//            userInfos.add(item);
//        }
//
//        fileReader.close();
//        csvParser.close();
//
//        return userInfos;
//    }
//
//    // generate dynamodb connection for operations with database.
//    public static final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
//            .withEndpointConfiguration(
//                    new AwsClientBuilder.EndpointConfiguration("http://localhost:4566/", "us-east-1"))
//            .enableEndpointDiscovery().build());
//
//    // name of table in dynamodb
//    static String forumTableName = "user_info";
//
//    // write item with batch fuction
//    public static Map<String, List<WriteRequest>> writeMultipleItemsBatchWrite(List<Item> userInfos)
//            throws AmazonDynamoDBException {
//
//        // Add a new item to Forum
//        TableWriteItems forumTableWriteItems = new TableWriteItems(forumTableName) // Forum
//                .withItemsToPut(userInfos);
//
//        logger.info("Making the request.");
//        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(forumTableWriteItems);
//
//        Map<String, List<WriteRequest>> unprocessedItems;
//
//        do {
//
//            // Check for unprocessed keys which could happen if you exceed
//            // provisioned throughput
//
//            unprocessedItems = outcome.getUnprocessedItems();
//
//            if (outcome.getUnprocessedItems().size() == 0) {
//                logger.info("No unprocessed items found");
//            } else {
//                logger.info("Retrieving the unprocessed items");
//                outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
//            }
//
//        } while (outcome.getUnprocessedItems().size() > 0);
//
//        return unprocessedItems;
//    }
//
//    // copy object in bucket. Using this function for moving object
//    public static CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationKey) {
//        return s3Client
//                .copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, sourceBucketName, destinationKey));
//    }
//
//    // delete object in bucket
//    public static void deleteObject(String sourceBucketName, String sourceKey) {
//        s3Client.deleteObject(sourceBucketName, sourceKey);
//    }
//
//    // generate destination key for moving object. Generate from sourcekey and
//    // destination(include processed,error)
//    public static String getDestinationKeyToMove(String sourceKey, String destination) {
//        StringBuilder stringBuilder = new StringBuilder(sourceKey);
//        return stringBuilder.replace(0, 4, destination).toString();
//    }
//
//    // move object from prefix data to processed
//    public static void moveObjectFromDataToProcessed(String sourceBucketName, String sourceKey) {
//        String destinationKey = getDestinationKeyToMove(sourceKey, "processed");
//        CopyObjectResult resultMoveObject = copyObject(sourceBucketName, sourceKey, destinationKey);
//        if (resultMoveObject.getETag() != null) {
//            deleteObject(sourceBucketName, sourceKey);
//            logger.info("move successs!!");
//        }
//    }
//
//    // move object from prefix data to error
//    public static void moveObjectFromDataToError(String sourceBucketName, String sourceKey) {
//        String destinationKey = getDestinationKeyToMove(sourceKey, "error");
//        CopyObjectResult resultMoveObject = copyObject(sourceBucketName, sourceKey, destinationKey);
//        if (resultMoveObject.getETag() != null) {
//            deleteObject(sourceBucketName, sourceKey);
//        }
//    }
//
//    public static void processFile(S3Object objecToProcess, String bucketName, String objectKey)
//            throws IOException, AmazonDynamoDBException {
//        // Readfile csv file to list
//        Consumer consumer = new Consumer();
//        List<Item> result = consumer.readCsvFile(objecToProcess.getObjectContent());
//
//        writeMultipleItemsBatchWrite(result);
//
//        moveObjectFromDataToProcessed(bucketName, objectKey);
//    }
//
//    // listen sqs messages and handle
//    @SqsListener(value = { "http://localhost:4566/000000000000/input-file-q" })
//    public void receiveMessage(S3EventNotification s3EventNotificationRecord) throws IOException {
//        S3EventNotification.S3Entity s3Entity = s3EventNotificationRecord.getRecords().get(0).getS3();
//        logger.info("Message Received using SQS Listner " + s3Entity.getObject().getKey() + "event "
//                + s3EventNotificationRecord.getRecords().get(0).getEventName());
//
//        // Filter message name
//        if (s3EventNotificationRecord.getRecords().get(0).getEventName().equals("ObjectCreated:Put")) {
//            String bucketName = s3EventNotificationRecord.getRecords().get(0).getS3().getBucket().getName();
//            String objectKey = s3Entity.getObject().getKey();
//            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectKey));
//
//            int timesToWriteWhenHasError = 10;
//
//            try {
//                processFile(object, bucketName, objectKey);
//            } catch (Exception e) {
//                do {
//                    try {
//                        processFile(object, bucketName, objectKey);
//                    } catch (Exception exceptionInLoop) {
//                        timesToWriteWhenHasError--;
//                        logger.error("error in loop " + exceptionInLoop.getMessage());
//                        continue;
//                    }
//                } while (timesToWriteWhenHasError > 0);
//                moveObjectFromDataToError(bucketName, objectKey);
//            }
//        }
//    }
//}
