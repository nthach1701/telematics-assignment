package com.example.telematicsassignment.entity;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.aws.messaging.config.QueueMessageHandlerFactory;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.stereotype.Component;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

@Component
public class consumer {

    private static final Logger logger = LoggerFactory.getLogger(consumer.class);

    // Value is populated by the region code.
    @Value("${cloud.aws.region.static}")
    private static String region;

    // Value is populated with the aws access key.
    @Value("${cloud.aws.credentials.access-key}")
    private String awsAccessKey;

    // Value is populated with the aws secret key
    @Value("${cloud.aws.credentials.secret-key}")
    private String awsSecretKey;

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

    // generate amazons3 connection for copying and moving object in bucket
    static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566/", region))
            .enablePathStyleAccess().build();

    // read csv file
    private static List<Item> readCsvFile(InputStream inputStream) throws IOException {
        BufferedReader fileReader = new BufferedReader(new InputStreamReader(inputStream));

        CSVParser csvParser = null;

        List<Item> UserInfos = new ArrayList<Item>();

        csvParser = new CSVParser(fileReader,
                CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
        Iterable<CSVRecord> csvRecords = csvParser.getRecords();
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");

        for (CSVRecord csvRecord : csvRecords) {
            String presenDate = formatter.format(new Date());
            Item item = new Item();
            item.withPrimaryKey("id", csvRecord.get("id"));
            item.withString("first", csvRecord.get("first"));
            item.withString("last", csvRecord.get("last"));
            item.withString("dob", csvRecord.get("dob"));
            item.withString("processed_time", presenDate);

            UserInfos.add(item);
        }

        fileReader.close();
        csvParser.close();

        return UserInfos;
    }

    // generate dynamodb connection for operations with database.
    static final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration("http://localhost:4566/", "us-east-1"))
            .enableEndpointDiscovery().build());

    // name of table in dynamodb
    static String forumTableName = "user_info";

    // write item with batch fuction
    private static void writeMultipleItemsBatchWrite(List<Item> userInfos) {
        try {

            // Add a new item to Forum
            TableWriteItems forumTableWriteItems = new TableWriteItems(forumTableName) // Forum
                    .withItemsToPut(userInfos);

            System.out.println("Making the request.");
            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(forumTableWriteItems);

            do {

                // Check for unprocessed keys which could happen if you exceed
                // provisioned throughput

                Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

                if (outcome.getUnprocessedItems().size() == 0) {
                    System.out.println("No unprocessed items found");
                } else {
                    System.out.println("Retrieving the unprocessed items");
                    outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                }

            } while (outcome.getUnprocessedItems().size() > 0);

        } catch (Exception e) {
            System.err.println("Failed to retrieve items: ");
            e.printStackTrace(System.err);
        }

    }

    // copy object in bucket. Using this function for moving object
    private static CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationKey) {
        CopyObjectResult result = s3Client
                .copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, sourceBucketName, destinationKey));
        return result;
    }

    // delete object in bucket
    private static void deleteObject(String sourceBucketName, String sourceKey) {
        s3Client.deleteObject(sourceBucketName, sourceKey);
    }

    // generate destination key for moving object. Generate from sourcekey and
    // destination(include processed,error)
    private static String getDestinationKeyToMove(String sourceKey, String destination) {
        StringBuilder stringBuilder = new StringBuilder(sourceKey);
        String destinationKey = stringBuilder.replace(0, 4, destination).toString();
        return destinationKey;
    }

    // move object from prefix data to processed
    private static void moveObjectFromDataToProcessed(String sourceBucketName, String sourceKey) {
        String destinationKey = getDestinationKeyToMove(sourceKey, "processed");
        CopyObjectResult resultMoveObject = copyObject(sourceBucketName, sourceKey, destinationKey);
        if (resultMoveObject.getETag() != null) {
            deleteObject(sourceBucketName, sourceKey);
            logger.info("move successs!!");
        }
    }

    // move object from prefix data to error
    private static void moveObjectFromDataToError(String sourceBucketName, String sourceKey) {
        String destinationKey = getDestinationKeyToMove(sourceKey, "error");
        CopyObjectResult resultMoveObject = copyObject(sourceBucketName, sourceKey, destinationKey);
        if (resultMoveObject.getETag() != null) {
            deleteObject(sourceBucketName, sourceKey);
        }
    }

    // listen sqs messages and handle
    @SqsListener(value = { "http://localhost:4566/000000000000/input-file-q" })
    public void receiveMessage(S3EventNotification s3EventNotificationRecord) throws IOException {
        S3EventNotification.S3Entity s3Entity = s3EventNotificationRecord.getRecords().get(0).getS3();
        logger.info("Message Received using SQS Listner " + s3Entity.getObject().getKey().toString() + "event "
                + s3EventNotificationRecord.getRecords().get(0).getEventName());

        // Filter message name
        if (s3EventNotificationRecord.getRecords().get(0).getEventName().equals("ObjectCreated:Put")) {
            String bucketName = s3EventNotificationRecord.getRecords().get(0).getS3().getBucket().getName();
            String objectKey = s3Entity.getObject().getKey();
            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, objectKey));

            try {
                // Readfile csv file to list
                List<Item> result = readCsvFile(object.getObjectContent());

                writeMultipleItemsBatchWrite(result);

                moveObjectFromDataToProcessed(bucketName, objectKey);
            } catch (Exception e) {
                moveObjectFromDataToError(bucketName, objectKey);
            }
        }
    }
}
