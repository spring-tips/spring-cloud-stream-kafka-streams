package com.example.wordcount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.java.Log;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class ConsumerApplication {

 public static void main(String[] args) {
	SpringApplication.run(ConsumerApplication.class, args);
 }

 @Log
 @RestController
 @Configuration
 @EnableBinding(KafkaStreamsProcessorX.class)
 public static class ConsumerConfig {

	private final QueryableStoreRegistry queryableStoreRegistry;

	public ConsumerConfig(QueryableStoreRegistry queryableStoreRegistry) {
	 this.queryableStoreRegistry = queryableStoreRegistry;
	}

	@StreamListener("input")
	public void process(KStream<String, DomainEvent> input) {
	 KTable<String, Long> longCountValue =
		 input
			 .groupBy((key, value) -> value.getBoardUuid())
			 .count();
	 longCountValue.foreach((key, value) -> log.info(String.format("%s shows up %s times.", key, value)));
	}

	@RequestMapping("/events")
	public String events() {
	 ReadOnlyKeyValueStore<String, String> topFiveStore =
		 queryableStoreRegistry.getQueryableStoreType("test-events-snapshots", QueryableStoreTypes.<String, String>keyValueStore());
	 return topFiveStore.get("12345");
	}
 }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class DomainEvent {
 private String eventType;
 private String boardUuid;
}


interface KafkaStreamsProcessorX {
 @Input("input")
 KStream<?, ?> input();
}

