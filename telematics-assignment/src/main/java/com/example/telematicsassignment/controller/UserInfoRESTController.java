package com.example.telematicsassignment.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.model.AmazonS3Exception;

@RestController
public class UserInfoRESTController {

    private static final Logger logger = LoggerFactory.getLogger(UserInfoRESTController.class);

    // generate dynamodb connection for operations with database.
    static final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration("http://localhost:4566/", "us-east-1"))
            .enableEndpointDiscovery().build());

    static String forumTableName = "user_info";

    @RequestMapping("/user-info/{id}")
    public ResponseEntity<String> GetUserInfo(@PathVariable("id") String id) {
        Table table = dynamoDB.getTable(forumTableName);
        Item item = null;
        try {
            item = table.getItem(new KeyAttribute("id", id));
        } catch (AmazonS3Exception e) {
            logger.error("object not found");
        }
        if (item != null)
            return new ResponseEntity<String>(item.toJSON(), HttpStatus.OK);
        else
            return new ResponseEntity<>("Object not found for id : " + id, HttpStatus.NOT_FOUND);
    }
}
