package com.globalids.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * Created by debasish paul on 30-10-2018.
 */
public class FileStreamProcessor {

    public static void processFile(String filePath) {

        System.setProperty("user.name", "hdp_gidz_key@GIDZ.COM");

        SparkSession sparkSession = SparkSessionBuilder.getSparkSession();
        /*sparkSession.sparkContext().getConf()
                .set("hadoop.security.authentication", "kerberos")
                .set("hadoop.security.authorization", "true")
                .set("spark.authenticate", "true")
                .set("spark.yarn.keytab", "file:///home/Hive cred/Hadoop_104/KeytabFile/104_hdp_gidz_key.keytab")
                .set("spark.yarn.principal", "nn/hortonworks1.gidz.com@GIDZ.COM");*/

//        Dataset<Row> avro = sparkSession.read().format("com.databricks.spark.avro")
//                .load("C:\\Users\\debasish paul\\Desktop\\test\\a.avro");
//
//        avro.show();

//        Dataset<Row> parquet = sparkSession.read().parquet("hdfs://192.168.44.112:8020/tmp/transform/data/*.parquet");
//        parquet.show();
//
//        Dataset<Row> ds2 = parquet.filter(col("str1").$eq$eq$eq("Str 1-6"));
//        ds2.write().mode(SaveMode.Append).parquet("hdfs://192.168.44.112:8020/tmp/transform/data2");

        Dataset<Row> parquet1 = sparkSession.read().parquet(filePath);
//        Dataset<Row> str11 = parquet1.groupBy(col("Sex")).count();
//        str11.show();

        parquet1.select(col(""));

        long str1 = parquet1.count();
        System.out.println("Total data count = " + str1);
    }

    public static void main(String[] args) {
        processFile("hdfs://192.168.44.112:8020/gid/all");
    }
}
