package com.example.telematicsassignment;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.AtomicLongAssert;
import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.example.telematicsassignment.repository.UserInfoRepository;
import com.example.telematicsassignment.service.AmazonClientService;
import com.example.telematicsassignment.service.SqsServiceImplement;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
class TelematicsAssignmentApplicationTests {

    @Autowired
    private SqsServiceImplement sqsServiceImplement;

    @Autowired
    private AmazonClientService amazonClientService;

    private static String formatDOB(Date DateToTransform) {
        return new SimpleDateFormat("M/d/yyyy").format(DateToTransform);
    }

    private static String formatDOBPresent(Date DateToTransform) {
        return new SimpleDateFormat("MM/dd/yyyy").format(DateToTransform);
    }

    static final List<Item> itemsOfTest = new ArrayList<Item>();
    static {
        itemsOfTest.add(new Item().withString("id", "1").withString("first", "Tom").withString("last", "Cruise")
                .withString("dob", formatDOB(new Date("7/3/1962")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "2").withString("first", "Emma").withString("last", "Watson")
                .withString("dob", formatDOB(new Date("4/15/1990")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "3").withString("first", "Sylvester").withString("last", "Stallone")
                .withString("dob", formatDOB(new Date("7/6/1946")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "4").withString("first", "Bernd").withString("last", "Leno")
                .withString("dob", formatDOB(new Date("3/4/1992")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "5").withString("first", "Erik").withString("last", "Lamela")
                .withString("dob", formatDOB(new Date("3/4/1992")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "6").withString("first", "Martin").withString("last", "Terrier")
                .withString("dob", formatDOB(new Date("3/4/1997")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "7").withString("first", "Antonio").withString("last", "Sanabria")
                .withString("dob", formatDOB(new Date("3/4/1996")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "8").withString("first", "Freddie").withString("last", "Woodman")
                .withString("dob", formatDOB(new Date("3/4/1997")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "9").withString("first", "Timo").withString("last", "Baumgartl")
                .withString("dob", formatDOB(new Date("3/4/1996")))
                .withString("processed_time", formatDOBPresent(new Date())));
        itemsOfTest.add(new Item().withString("id", "10").withString("first", "Landon").withString("last", "Donovan")
                .withString("dob", formatDOB(new Date("3/4/1982")))
                .withString("processed_time", formatDOBPresent(new Date())));

    }
    
    @Test
    void testMoveObjectFromDataToProcessed() {
        sqsServiceImplement.moveObjectFromDataToProcessed("input-file", "data/command2");
        assertEquals(false, amazonClientService.doesObjectExist("input-file", "data/command2"));
    }

    @Test
    void testmoveObjectFromDataToError() {
        sqsServiceImplement.moveObjectFromDataToError("input-file", "data/command3");
        assertEquals(false, amazonClientService.doesObjectExist("input-file", "data/command3"));
    
    }

    @Test
    void testReadCsvFile() throws FileNotFoundException, IOException {
        List<Item> listsItemFromConsumer = new ArrayList<Item>();
        listsItemFromConsumer = sqsServiceImplement
                .readCsvFile(new FileInputStream(new File("/home/thachnguyen/Desktop/localstack/data2.csv")));
        assertThat(itemsOfTest, is(listsItemFromConsumer));
    }

    @Test
    void testwriteMultipleItemsBatchWrite() {
        Map<String, List<WriteRequest>> testUnProcessedItems = sqsServiceImplement
                .writeMultipleItemsBatchWrite(itemsOfTest);
        assertEquals(true, testUnProcessedItems.isEmpty());
    }

    @Test
    void testCopyObject() {
        CopyObjectResult resultCopyObjectInBucket = amazonClientService.copyObject("input-file", "error/command",
                "test/command");
        assertNotEquals("", resultCopyObjectInBucket.getETag());
        resultCopyObjectInBucket = amazonClientService.copyObject("input-file", "error/command", "test/");
        assertEquals("error/command",
                amazonClientService.getObject("input-file", "error/command").getKey());
    }

    @Test
    void testDeleteObject() {
        amazonClientService.deleteObject("input-file", "error/command");
        Assertions.assertThrows(AmazonS3Exception.class, () -> {
            S3Object testDeleteObjectFunction = amazonClientService.getObject("input-file", "error/command");
        });
    }
    
    @Test
    void testGetDestinationKeyToMove() {
        String result = sqsServiceImplement.getDestinationKeyToMove("test/command", "error");
        assertEquals("error/command", result);
    }
    
   
    
//    @Autowired
//    private UserInfoRepository userInfoRepository;
//    @Test
//    void testProcessFile() throws IOException {
//        S3Object s3ObjectToProcess = amazonClientService.getObject("input-file", "data/data2.csv");
//        sqsServiceImplement.processFile(s3ObjectToProcess, "input-file", "data/data2.csv");
//        
//        List<Item> resultTocheck = sqsServiceImplement.readCsvFile(new FileInputStream(new File("/home/thachnguyen/Desktop/localstack/data2.csv")));
//        assertEquals(10,resultTocheck.stream().toArray().length);
//        assertEquals(10,userInfoRepository.batchGetItems(param));
//    }
}
