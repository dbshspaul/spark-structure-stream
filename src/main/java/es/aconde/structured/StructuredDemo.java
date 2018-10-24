package es.aconde.structured;

import com.databricks.spark.avro.SchemaConverters;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.collect_set;

/**
 * Structured streaming demo using Avro'ed Kafka topic as input
 *
 * @author Angel Conde
 */
public class StructuredDemo {

    private static Injection<GenericRecord, byte[]> recordInjection;
    private static StructType type;
    private static Schema.Parser parser = new Schema.Parser();
    private static Schema schema = parser.parse(GeneratorDemo.USER_SCHEMA);

    static { //once per VM, lazily
        recordInjection = GenericAvroCodecs.toBinary(schema);
        type = (StructType) SchemaConverters.toSqlType(schema).dataType();

    }

    public static void main(String[] args) throws StreamingQueryException {
        //set log4j programmatically
        LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        //configure Spark
        SparkConf conf = new SparkConf()
                .setAppName("kafka-structured")
                .setMaster("local[*]");

        //initialize spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        //reduce task number
        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");

        //data stream from kafka
        Dataset<Row> ds1 = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "mytopic")
//                .option("startingOffsets", "earliest")
                .load();

        //start the streaming query
        sparkSession.udf().register("deserialize", (byte[] data) -> {

            GenericRecord record = recordInjection.invert(data).get();
            record.getSchema();
            List<Schema.Field> fields = record.getSchema().getFields();
            Object[] recordArr = new Object[fields.size()];

            for (int i = 0; i < fields.size(); i++) {
                recordArr[i] = record.get(fields.get(i).name()) == null ? "" : record.get(fields.get(i).name()).toString();
            }
            return RowFactory.create(recordArr);
        }, DataTypes.createStructType(type.fields()));

        ds1.printSchema();
        Dataset<Row> ds2 = ds1
                .select("value").as(Encoders.BINARY())
                .selectExpr("deserialize(value) as rows")
                .select("rows.*");

        ds2.printSchema();

        Dataset<Row> ds3 = ds2.groupBy("str1").agg(collect_set("str2"), collect_list("int1"));
        ds3.printSchema();


        ds3.writeStream()
                .option("path", "D:/sparkout/data")
                .option("checkpointLocation", "checkpoint")
                .queryName("Test query")
//                .outputMode("complete")
                .foreach(new FileWritter())
                .outputMode("update")
                .start()
                .awaitTermination();
    }


}
