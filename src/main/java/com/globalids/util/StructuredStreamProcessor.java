package com.globalids.util;

import com.databricks.spark.avro.SchemaConverters;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;

/**
 * Created by debasish paul
 */
public class StructuredStreamProcessor{

    private static Injection<GenericRecord, byte[]> recordInjection;
    private static StructType type;
    private static Schema schema;

    public StructuredStreamProcessor(String schemaJSON) {
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(schemaJSON);
        recordInjection = GenericAvroCodecs.toBinary(schema);
        type = (StructType) SchemaConverters.toSqlType(schema).dataType();
    }

    public void startStreamingJob(String topic, String filePath) throws StreamingQueryException {
        LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
//                .setJars(new String[]{})
                .setAppName("kafka-structured-transformation")
                .setMaster("local[*]");
//                .setMaster("spark://192.168.33.207:7077");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

//        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");
        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "30");
        sparkSession.sqlContext().setConf("spark.default.parallelism", "30");

        //data stream from kafka
        Dataset<Row> ds1 = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.33.207:9092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load();

        List<String> columnNames = new ArrayList<>();
        List<Schema.Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i).name();
            columnNames.add(fieldName);
        }

        sparkSession.udf().register("deserialize", (UDF1<byte[], Object>) data -> {

            GenericRecord record = recordInjection.invert(data).get();
            Object[] recordArr = new Object[columnNames.size()];

            for (int i = 0; i < columnNames.size(); i++) {
                String fieldName = columnNames.get(i);
                if (fieldName.equalsIgnoreCase("AUDITCOLUMN")) {
                    recordArr[i] = System.currentTimeMillis();
                } else {
                    recordArr[i] = record.get(fieldName) == null ? "" : record.get(fieldName).toString();
                }
            }
            return RowFactory.create(recordArr);
        }, DataTypes.createStructType(type.fields()));

        ds1.printSchema();
        Dataset<Row> ds2 = ds1
                .select("value").as(Encoders.BINARY())
                .selectExpr("deserialize(value) as rows")
                .select("rows.*");
        ds2.printSchema();

        Column columns[] = new Column[columnNames.size()];
        for (int i = 0, j = 0; i < columnNames.size(); i++) {
            columns[j++] = collect_set(columnNames.get(i));
        }
//        Dataset<Row> ds3 = ds2.groupBy("ACCOUNTNUMBER").agg(collect_set("ACCOUNTNUMBER"), columns);
//        ds3.printSchema();
//        ds3.writeStream()
////                .option("path", "D:/sparkout/data")
//                .option("checkpointLocation", "checkpoint")
//                .queryName("query_plan_1")
//                .foreach(new AVROFileWritter(schema.toString(), filePath, columnNames))
//                .outputMode("complete").start().awaitTermination();


        ds2.filter(col("GENDER").$eq$eq$eq("M")).writeStream()
                .option("path", "D:/sparkout/data")
                .option("checkpointLocation", "checkpoint")
                .queryName("query plan for dump all")
                .foreach(new AVROFileWritter(schema.toString(), filePath, columnNames))
                .outputMode("append")
                .start()
                .awaitTermination();
    }

    public static void main(String[] args) {
        StructuredStreamProcessor processor = new StructuredStreamProcessor("{\"type\":\"record\",\"name\":\"rt\",\"fields\":[{\"name\":\"GENDER\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ACCOUNTNUMBER\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"EMPLOYEE_ID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"BANK_IDNTFIER_ID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADDRESS_LINE_1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADDRESS_LINE_2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADDRESS_LINE_3\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"CITY\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"STATE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"POSTAL_CODE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"COUNTRY\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ADR_TYPE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"BANK\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"PHONE_NO\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"EMAIL_ADDRESS\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"DESIGNATION\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"PASSPORT_NUMBER\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"NATIONAL_IDENTIFICATION_NUMBER\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"DRIVERS_LICENSE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"NATIONALITY\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"BIOMETREIC\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"INSTANCEID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"DATE\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"FIRST_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"LAST_NAME\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"AUDITCOLUMN\",\"type\":[\"null\",\"long\"],\"default\":null}]}");
        try {
            processor.startStreamingJob("GID_OBG_STF", "C:\\Users\\debasish paul\\Desktop\\test\\test.avro");
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
