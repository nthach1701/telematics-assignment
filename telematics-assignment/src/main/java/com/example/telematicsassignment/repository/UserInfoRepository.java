package com.example.telematicsassignment.repository;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchGetItemSpec;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

@Repository
public class UserInfoRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(UserInfoRepository.class);

    private DynamoDB dynamoDB;
    
    // name of table in dynamodb
    private String forumTableName = "user_info";

    @PostConstruct
    public void initializeDynamoDB() {
        dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration("http://localhost:4566/", "us-east-1"))
                .enableEndpointDiscovery().build());
    }
    
    public Map<String, List<WriteRequest>> writeMultipleItemsBatchWrite(List<Item> userInfos){
     // Add a new item to Forum
        TableWriteItems forumTableWriteItems = new TableWriteItems(forumTableName) // Forum
                .withItemsToPut(userInfos);

        logger.info("Making the request.");
        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(forumTableWriteItems);

        Map<String, List<WriteRequest>> unprocessedItems;

        do {

            // Check for unprocessed keys which could happen if you exceed
            // provisioned throughput

            unprocessedItems = outcome.getUnprocessedItems();

            if (outcome.getUnprocessedItems().size() == 0) {
                logger.info("No unprocessed items found");
            } else {
                logger.info("Retrieving the unprocessed items");
                outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
            }

        } while (outcome.getUnprocessedItems().size() > 0);

        return unprocessedItems;
    }
    
    public List<Item> batchGetItems(List<String> param){
        TableKeysAndAttributes keyAndAttributeToGet = new TableKeysAndAttributes(forumTableName);
        BatchGetItemOutcome outcome = dynamoDB.batchGetItem(keyAndAttributeToGet);
        List<Item> items = outcome.getTableItems().get(forumTableName);
        return items;
    }
}
