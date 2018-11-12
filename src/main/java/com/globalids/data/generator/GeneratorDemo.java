package com.globalids.data.generator;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.Random;
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
            + "\"name\":\"rt\","
            + "\"fields\":["
            + "  { \"name\":\"Name\", \"type\":\"string\" },"
            + "  { \"name\":\"Address1\", \"type\":\"string\" },"
            + "  { \"name\":\"Address2\", \"type\":\"string\" },"
            + "  { \"name\":\"Address3\", \"type\":\"string\" },"
            + "  { \"name\":\"Address4\", \"type\":\"string\" },"
            + "  { \"name\":\"Address5\", \"type\":\"string\" },"
            + "  { \"name\":\"Description1\", \"type\":\"string\" },"
            + "  { \"name\":\"Description2\", \"type\":\"string\" },"
            + "  { \"name\":\"Description3\", \"type\":\"string\" },"
            + "  { \"name\":\"Sex\", \"type\":\"string\" },"
            + "  { \"name\":\"Age\", \"type\":\"string\" },"
            + "  { \"name\":\"Phone1\", \"type\":\"string\" },"
            + "  { \"name\":\"Phone2\", \"type\":\"string\" },"
            + "  { \"name\":\"Phone3\", \"type\":\"string\" },"
            + "  { \"name\":\"Dob\", \"type\":\"string\" },"
            + "  { \"name\":\"Doj\", \"type\":\"string\" },"
            + "  { \"name\":\"CustomerId\", \"type\":\"string\" },"
            + "  { \"name\":\"FBid\", \"type\":\"string\" },"
            + "  { \"name\":\"Height\", \"type\":\"string\" },"
            + "  { \"name\":\"Index\", \"type\":\"string\" }"
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
            avroRecord.put("Name", generateWord(1));
            avroRecord.put("Address1", generateWord(20));
            avroRecord.put("Address2", generateWord(20));
            avroRecord.put("Address3", generateWord(20));
            avroRecord.put("Address4", generateWord(20));
            avroRecord.put("Address5", generateWord(20));
            avroRecord.put("Description1", generateWord(15));
            avroRecord.put("Description2", generateWord(20));
            avroRecord.put("Description3", generateWord(20));
            avroRecord.put("Sex", random.nextInt(10) % 2 == 0 ? "M" : "F");
            avroRecord.put("Age", String.valueOf(random.nextDouble(1, 80)));
            avroRecord.put("Phone1", String.valueOf(random.nextLong(9000000000l,9999999999l)));
            avroRecord.put("Phone2", String.valueOf(random.nextLong(9000000000l,9999999999l)));
            avroRecord.put("Phone3", String.valueOf(random.nextLong(9000000000l,9999999999l)));
            avroRecord.put("Dob", generateDate());
            avroRecord.put("Doj", generateDate());
            avroRecord.put("CustomerId", generateWord(1));
            avroRecord.put("FBid", generateWord(1));
            avroRecord.put("Height", String.valueOf(random.nextDouble(5, 7)));
            avroRecord.put("Index", generateWord(1));

            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("GID_OBJ_mytopic", bytes);
            producer.send(record);
            System.out.println((i + 1)/* + " = " + avroRecord*/);
            Thread.sleep(7000);
        }
    }

    private static String generateWord(int numberOfWords) {
        String randomStrings = "";
        Random random = new Random();
        for (int i = 0; i < numberOfWords; i++) {
            char[] word = new char[random.nextInt(8) + 3];
            for (int j = 0; j < word.length; j++) {
                word[j] = (char) ('a' + random.nextInt(26));
            }
            randomStrings += new String(word) + " ";
        }
        return randomStrings;
    }

    public static String generateDate() {

        GregorianCalendar gc = new GregorianCalendar();

        int year = randBetween(1950, 2018);

        gc.set(gc.YEAR, year);

        int dayOfYear = randBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));

        gc.set(gc.DAY_OF_YEAR, dayOfYear);

        return gc.get(gc.YEAR) + "-" + (gc.get(gc.MONTH) + 1) + "-" + gc.get(gc.DAY_OF_MONTH);

    }

    public static int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }
}
