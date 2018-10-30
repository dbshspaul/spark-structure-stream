package com.globalids.data.generator;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.SplittableRandom;

/**
 * Created by debasish paul
 */
public class GeneratorDemo {

    /**
     * Avro defined schema
     */
    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"alarm\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"string\" }"
            + "]}";

//    public static final String USER_SCHEMA = "{\"type\":\"record\",\"name\":\"rt\",\"fields\":[{\"name\":\"GENDER\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ACCOUNTNUMBER\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"EMPLOYEE_ID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"BANK_IDNTFIER_ID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADDRESS_LINE_1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADDRESS_LINE_2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADDRESS_LINE_3\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"CITY\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"STATE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"POSTAL_CODE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"COUNTRY\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADR_TYPE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"BANK\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"PHONE_NO\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"EMAIL_ADDRESS\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"DESIGNATION\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"PASSPORT_NUMBER\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"NATIONAL_IDENTIFICATION_NUMBER\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"DRIVERS_LICENSE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"NATIONALITY\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"BIOMETREIC\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"INSTANCEID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"DATE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"FIRST_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"LAST_NAME\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

    /**
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        SplittableRandom random = new SplittableRandom();

        for (int i = 0; i < 100; i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("str1", "Str 1-" + random.nextInt(10));
            avroRecord.put("str2", "Str 2-" + random.nextInt(1000));
            avroRecord.put("int1", String.valueOf(random.nextInt(10000)));

            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("mytopic", bytes);
            producer.send(record);
            System.out.println((i + 1) + " = " + avroRecord);
            Thread.sleep(7000);
        }

    }
}
