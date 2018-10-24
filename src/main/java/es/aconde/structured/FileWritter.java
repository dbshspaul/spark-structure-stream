package es.aconde.structured;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by debasish paul on 24-10-2018.
 */
public class FileWritter extends ForeachWriter<Row> {
    FileWriter writer;

    @Override
    public boolean open(long l, long l1) {
        try {
            writer = new FileWriter("C:\\Users\\debasish paul\\Desktop\\test\\test.txt", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void process(Row row) {
        try {
            writer.write(row.toString() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(Throwable throwable) {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

