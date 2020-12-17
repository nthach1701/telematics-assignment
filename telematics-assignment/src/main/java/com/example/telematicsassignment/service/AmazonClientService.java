package com.example.telematicsassignment.service;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.S3Object;

@Service
public class AmazonClientService {
    // Value is populated by the region code.
    @Value("${cloud.aws.region.static}")
    private static String region;

    private AmazonS3 s3Client;

    // generate amazons3 connection for copying and moving object in bucket
    @PostConstruct
    private void initializeAmazonS3Client() {
        s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566/", region))
                .enablePathStyleAccess().build();
    }
    
 
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationKey) {
        return s3Client.copyObject(sourceBucketName, sourceKey, sourceBucketName, destinationKey);
    }

 
    public void deleteObject(String sourceBucketName, String sourceKey) {
        s3Client.deleteObject(sourceBucketName, sourceKey);
    }
    
    public S3Object getObject(String bucketName, String sourceKey) {
        return s3Client.getObject(bucketName,sourceKey);
    }
    
    public boolean doesObjectExist(String bucketName, String sourceKey) {
        return s3Client.doesObjectExist(bucketName,sourceKey);
    }
}
