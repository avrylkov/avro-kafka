package com.example.model;

import org.apache.avro.Schema;

public class SchemaRepository {

    private static final String SCHEMA = "{\"namespace\": \"avro\",\n" +
            "\"type\": \"record\",\n" +
            "\"name\": \"Person\",\n" +
            "\"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"age\",  \"type\": [\"int\", \"null\"]}\n" +
            "]\n" +
            "}\n";

    private static final Schema SCHEMA_OBJECT = new Schema.Parser().parse(SCHEMA);

    private static SchemaRepository INSTANCE = new SchemaRepository();

    public static SchemaRepository instance() {
      return INSTANCE;
    }

    public Schema getSchemaObject() {
        return SCHEMA_OBJECT;
    }

}
