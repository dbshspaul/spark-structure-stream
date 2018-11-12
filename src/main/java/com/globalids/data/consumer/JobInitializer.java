package com.globalids.data.consumer;

import com.globalids.spark.SparkSessionBuilder;
import com.globalids.spark.StructuredStreamProcessor;
import com.globalids.transform.ParseFilter;
import com.globalids.transform.etl.Transformation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by debasish paul on 25-10-2018.
 */
public class JobInitializer {
    private static final Logger LOGGER = Logger.getLogger(JobInitializer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("data_transformation_initiator"));


        while (true) {
            ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(5000);
            consumerRecords.forEach(consumerRecord -> {
                try {
                    Map<String, String> prerequisiteData = deserializer(consumerRecord.value());
                    String instructionType = prerequisiteData.get("instruction-type");
                    if (instructionType.equalsIgnoreCase("movement")) {
                        LOGGER.info("movement instruction received.");
                        String schema = prerequisiteData.get("schema");
                        String topic = prerequisiteData.get("topic");
                        String targetPath = prerequisiteData.get("target_path");
                        System.out.println("schema = " + schema);
                        System.out.println("target_path = " + targetPath);
                        System.out.println("topic = " + topic);
                        new Thread(() -> {
                            boolean flag = true;
                            while (flag) {
                                try {
                                    boolean queryActive = SparkSessionBuilder.isJobActive("Dump All Data");
                                    if (!queryActive) {
                                        LOGGER.info("Executing new job.");
                                        StructuredStreamProcessor.startStreamingJob(topic, targetPath, schema);
                                        flag = false;
                                    } else {
                                        LOGGER.info("Current job is active.");
                                    }
                                } catch (StreamingQueryException e) {
                                    e.printStackTrace();
                                }
                                try {
                                    LOGGER.info("Retry in 30 sec.");
                                    Thread.sleep(30000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }).start();
                    } else if (instructionType.equalsIgnoreCase("transform")) {
                        LOGGER.info("transform instruction received.");
                        String targetPath = prerequisiteData.get("target_path");
                        String sourcePath = prerequisiteData.get("source_path");
                        System.out.println("target path = " + targetPath);
                        System.out.println("source path = " + sourcePath);
                        String analysisData = prerequisiteData.get("data");
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String,String> transformation = mapper.readValue(analysisData, HashMap.class);
                        ParseFilter.parse(transformation);


//                        FileStreamProcessor.processFile(targetPath);
                    } else if (instructionType.equalsIgnoreCase("stop")) {
                        LOGGER.info("stop instruction received.");
                        SparkSessionBuilder.terminateJob("Dump All Data");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static Map<String, String> deserializer(byte[] data) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = mapper.readValue(new String(data), Map.class);
        return map;
    }
}
