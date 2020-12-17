package com.example.telematicsassignment.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3Object;

public interface SqsService {
    public List<Item> readCsvFile(InputStream inputStream) throws IOException;

    public Map<String, List<WriteRequest>> writeMultipleItemsBatchWrite(List<Item> userInfos);

    public String getDestinationKeyToMove(String sourceKey, String destination);

    public void moveObjectFromDataToProcessed(String sourceBucketName, String sourceKey);

    public void moveObjectFromDataToError(String sourceBucketName, String sourceKey);

    public void processFile(S3Object objecToProcess, String bucketName, String objectKey) throws IOException;

    public void receiveMessage(S3EventNotification s3EventNotificationRecord);
}
