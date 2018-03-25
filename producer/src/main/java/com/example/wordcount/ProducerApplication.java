package com.example.wordcount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.java.Log;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Instant;
import java.util.*;

@Log
@SpringBootApplication
public class ProducerApplication {

 public static void main(String[] args) {
	SpringApplication.run(ProducerApplication.class, args);
 }

 @Configuration
 public static class ProducerConfig {

	@Bean
	ProducerFactory<String, DomainEvent> myProducerFactory(KafkaProperties properties) {
	 Map<String, Object> producerProperties = properties.buildProducerProperties();
	 DefaultKafkaProducerFactory<String, DomainEvent> factory = new DefaultKafkaProducerFactory<>(producerProperties);
	 factory.setKeySerializer(new StringSerializer());
	 factory.setValueSerializer(new JsonSerializer<>());
	 String transactionIdPrefix = properties.getProducer().getTransactionIdPrefix();
	 Optional.ofNullable(transactionIdPrefix).ifPresent(factory::setTransactionIdPrefix);
	 return factory;
	}

	@Bean
	KafkaTemplate<String, DomainEvent> template(
		ProducerFactory<String, DomainEvent> producerFactory) {
	 return new KafkaTemplate<>(producerFactory);
	}

	@Bean
	ApplicationRunner producer(KafkaTemplate<String, DomainEvent> template) {

	 List<String> uids = new ArrayList<>();
	 for (int i = 0; i < 5; i++) {
		uids.add(UUID.randomUUID().toString());
	 }

	 return args -> {
		for (int i = 0; i < 10; i++) {
		 int randomPtr = (int) (Math.random() * (uids.size() - 1));
		 DomainEvent ddEvent = new DomainEvent(Instant.now().toString(), uids.get(randomPtr));
		 log.info("sending " + DomainEvent.class.getName() + ": " + ddEvent.toString());
		 template.send("domain-events", ddEvent);
		}
	 };
	}
 }
}

@SuppressWarnings("ignored")
class DomainEventSerde extends JsonSerde<DomainEvent> {

 public DomainEventSerde() {
	super(DomainEvent.class);
 }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
class DomainEvent {
 private String eventType, boardUuid;
}


interface KafkaStreamsProcessorX {
 @Input("input")
 KStream<?, ?> input();
}
