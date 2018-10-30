package com.globalids.spark;

import com.databricks.spark.avro.SchemaConverters;
import com.globalids.file.writter.AVROFileWritter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import test.JarUtility;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.collect_set;

/**
 * Created by debasish paul
 */
public class StructuredStreamProcessor {

    private static StructType type;
    private static Schema schema;

    public StructuredStreamProcessor(String schemaJSON) {
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(schemaJSON);
        type = (StructType) SchemaConverters.toSqlType(schema).dataType();
    }

    public void startStreamingJob(String topic, String filePath) throws StreamingQueryException {
        LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setJars(JarUtility.getAllJarPath())
                .setAppName("kafka-structured-transformation")
                .setMaster("local[*]");
//                .setMaster("spark://192.168.33.207:7077");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");
//        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "30");
//        sparkSession.sqlContext().setConf("spark.default.parallelism", "30");

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
        ds1.printSchema();

        Dataset<Row> ds2 = ds1
                .select("value").as(Encoders.BINARY())
                .map(bytes -> {
                    Object[] recordArr = new Object[columnNames.size()];
                    GenericRecord record = deserialize(bytes, schema);

                    for (int i = 0; i < columnNames.size(); i++) {
                        String fieldName = columnNames.get(i);
                        if (fieldName.equalsIgnoreCase("AUDITCOLUMN")) {
                            recordArr[i] = System.currentTimeMillis();
                        } else {
                            recordArr[i] = record.get(fieldName) == null ? "" : record.get(fieldName).toString();
                        }
                    }
                    return RowFactory.create(recordArr);
                }, RowEncoder.apply(type));
        ds2.printSchema();

        Column columns[] = new Column[columnNames.size()];
        for (int i = 0, j = 0; i < columnNames.size(); i++) {
            columns[j++] = collect_set(columnNames.get(i));
        }
        ds2.writeStream()
                .option("path", "D:/sparkout/data")
                .option("checkpointLocation", "checkpoint")
                .queryName("query plan for dump all")
                .foreach(new AVROFileWritter(schema.toString(), filePath, columnNames))
                .outputMode("append")
                .start()
                .awaitTermination();
    }


    public static GenericRecord deserialize(byte[] data, Schema schema) throws Exception {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
        return reader.read(null, decoder);
    }


    public static void main(String[] args) {
        StructuredStreamProcessor processor = new StructuredStreamProcessor("{"
                + "\"type\":\"record\","
                + "\"name\":\"alarm\","
                + "\"fields\":["
                + "  { \"name\":\"str1\", \"type\":\"string\" },"
                + "  { \"name\":\"str2\", \"type\":\"string\" },"
                + "  { \"name\":\"int1\", \"type\":\"string\" }"
                + "]}");
        try {
            processor.startStreamingJob("mytopic", "C:\\Users\\debasish paul\\Desktop\\test\\test.avro");
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
