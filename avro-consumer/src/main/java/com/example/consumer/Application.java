package com.example.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@KafkaListener(topics = "test")
	public void listen(ConsumerRecord<?, ?> cr) throws Exception {
		List<GenericRecord> records = (List<GenericRecord>) cr.value();

		records.forEach(r -> System.out.println(r.toString()));
	}

}
