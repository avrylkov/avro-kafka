package com.example.producer;

import com.example.model.SchemaRepository;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.util.ArrayList;
import java.util.List;

@ShellComponent
public class Commands {

    private List<GenericRecord> records = new ArrayList<>();

    final private KafkaTemplate template;

    public Commands(KafkaTemplate template) {
      this.template = template;
    }

    @ShellMethod("add user to send")
    public void add(String name, int age) {
        GenericRecord record = new GenericData.Record(SchemaRepository.instance().getSchemaObject());
        record.put("name", name);
        record.put("age", age);

        records.add(record);
    }

    @ShellMethod("send user to Kafka")
    public void send() {
        template.setDefaultTopic("test");
        template.sendDefault("1", records);
        template.flush();
        records.clear();
    }


}
