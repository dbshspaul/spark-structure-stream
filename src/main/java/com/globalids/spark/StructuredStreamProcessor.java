package com.globalids.spark;

import com.databricks.spark.avro.SchemaConverters;
import com.globalids.data.generator.GeneratorDemo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by debasish paul
 */
public class StructuredStreamProcessor {
    private static final Logger LOGGER = Logger.getLogger(StructuredStreamProcessor.class);


    public static void startStreamingJob(String topic, String filePath, String schemaJSON) throws StreamingQueryException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaJSON);
        StructType type = (StructType) SchemaConverters.toSqlType(schema).dataType();


        List<String> columnNames = new ArrayList<>();
        List<Schema.Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i).name();
            columnNames.add(fieldName);
        }

        SparkSession sparkSession = SparkSessionBuilder.getSparkSession();
        Dataset<Row> kafkaDataSet = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.33.207:9092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load();
        DataStreamWriter<Row> streamWriter = kafkaDataSet.select("value").as(Encoders.BINARY())
                .map(bytes -> {
                    Schema.Parser parser1 = new Schema.Parser();
                    Schema schema1 = parser1.parse(schemaJSON);
                    Object[] recordArr = new Object[columnNames.size()];
                    GenericRecord record = deserialize(bytes, schema1);

                    for (int i = 0; i < columnNames.size(); i++) {
                        String fieldName = columnNames.get(i);
                        if (fieldName.equalsIgnoreCase("AUDITCOLUMN")) {
                            recordArr[i] = System.currentTimeMillis();
                        } else {
                            recordArr[i] = record.get(fieldName) == null ? "" : record.get(fieldName).toString();
                        }
                    }
                    return RowFactory.create(recordArr);
                }, RowEncoder.apply(type)).writeStream()
                .option("path", filePath)
                .option("checkpointLocation", "checkpoint/" + topic)
                .queryName("Dump Data for " + topic.substring(topic.indexOf("GID_OBJ_")))
                .format("parquet")
                .outputMode("append");

        streamWriter.start().awaitTermination();
    }

    public static GenericRecord deserialize(byte[] data, Schema schema) throws Exception {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
        return reader.read(null, decoder);
    }


    public static void main(String[] args) {
        try {
            startStreamingJob("GID_OBJ_mytopic", "hdfs://192.168.44.112:8020/gid/all", GeneratorDemo.USER_SCHEMA);
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
