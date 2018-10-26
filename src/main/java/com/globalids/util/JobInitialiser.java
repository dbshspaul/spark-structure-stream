package com.globalids.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by debasish paul on 25-10-2018.
 */
public class JobInitialiser {
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
                    String schema = prerequisiteData.get("schema");
                    String topic = prerequisiteData.get("topic");
                    String target_path = prerequisiteData.get("target_path");
                    System.out.println("schema = " + schema);
                    System.out.println("target_path = " + target_path);
                    System.out.println("topic = " + topic);
                    new Thread(() -> {
                        StructuredStreamProcessor processor = new StructuredStreamProcessor(schema);
                        try {
                            processor.startStreamingJob(topic,target_path);
                        } catch (StreamingQueryException e) {
                            e.printStackTrace();
                        }
                    }).start();
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
