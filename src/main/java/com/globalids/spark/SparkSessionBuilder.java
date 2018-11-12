package com.globalids.spark;

import com.globalids.util.JarUtility;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * Created by debasish paul on 09-11-2018.
 */
public class SparkSessionBuilder {
    private static final Logger LOGGER = Logger.getLogger(SparkSessionBuilder.class);
    private static SparkSession sparkSession;

    private static void getOrCreateSparkSession() {
        LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
        LogManager.getLogger("akka").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setJars(JarUtility.getAllJarPath())
                .setAppName("Spark ETL")
                .set("spark.streaming.stopGracefullyOnShutdown", "true")
                .setMaster("spark://192.168.44.112:7077");

        sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "30");
        sparkSession.sqlContext().setConf("spark.default.parallelism", "30");
    }

    public static void terminateJob(String queryName){
        if (sparkSession != null) {
            StreamingQuery[] active = sparkSession.streams().active();
            for (StreamingQuery streamingQuery : active) {
                if (streamingQuery.name().equalsIgnoreCase(queryName)){
                    while (streamingQuery.status().isTriggerActive()) {
                        try {
                            LOGGER.warn("Job is active, checking again in 10 sec.");
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    LOGGER.warn("Terminating job.");
                    streamingQuery.stop();
                    break;
                }
            }
        }else {
            LOGGER.info("No alive Spark session found.");
        }
    }

    public static boolean isJobActive(String queryName){
        if (sparkSession != null) {
            StreamingQuery[] active = sparkSession.streams().active();
            for (StreamingQuery streamingQuery : active) {
                if (streamingQuery.name().equalsIgnoreCase(queryName)){
                    return true;
                }
            }
        }else {
            LOGGER.info("No alive Spark session found.");
        }
        return false;
    }

    public static SparkSession getSparkSession() {
        if (sparkSession == null) {
            getOrCreateSparkSession();
        }
        return sparkSession;
    }
}
