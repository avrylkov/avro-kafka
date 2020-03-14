package com.example.model;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvroGenericRecordDeserializer implements Deserializer {

    private Schema schema = null;

    @Override
    public void configure(Map configs, boolean isKey) {
        schema = (Schema) configs.get("SCHEMA");
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        SeekableByteArrayInput arrayInput = new SeekableByteArrayInput(bytes);
        List<GenericRecord> records = new ArrayList<>();

        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<>(arrayInput, datumReader);
            while (dataFileReader.hasNext()) {
                GenericRecord record = dataFileReader.next();
                records.add(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;

    }

}
