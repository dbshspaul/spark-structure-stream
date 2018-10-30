package com.globalids.file.writter;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by debasish paul on 24-10-2018.
 */
public class AVROFileWritter extends ForeachWriter<Row> {
    private static Schema.Parser parser = new Schema.Parser();
    private static Schema schema;
    private DataFileWriter<GenericRecord> dataFileWriter = null;
    private String avroFilePath;
    private java.util.List<String> columnNames;

    public AVROFileWritter(String schema, String avroFilePath, List<String> columnNames) {
        this.schema = parser.parse(schema);
        this.avroFilePath = avroFilePath;
        this.columnNames = columnNames;
    }

    @Override
    public boolean open(long l, long l1) {
        try {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            dataFileWriter = new DataFileWriter<>(datumWriter);
            File avroFile = new File(avroFilePath);
            if (!avroFile.exists()) {
                dataFileWriter.create(schema, avroFile);
            } else {
                dataFileWriter.appendTo(avroFile);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void process(Row row) {
        GenericRecord someRecord = new GenericData.Record(schema);
        for (String columnName : columnNames) {
            int i = row.fieldIndex(columnName);
            someRecord.put(columnName, row.get(i));
        }
        try {
            dataFileWriter.append(someRecord);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(Throwable throwable) {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

