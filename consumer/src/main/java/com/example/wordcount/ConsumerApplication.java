package com.example.wordcount;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.java.Log;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO conjure up an example where we join with another stream somehow
 */
@Log
@RestController
@Configuration
@EnableBinding(SimpleKafkaStreamsProcessor.class)
@SpringBootApplication
public class ConsumerApplication {

 public static void main(String[] args) {
	SpringApplication.run(ConsumerApplication.class, args);
 }

 private final QueryableStoreRegistry queryableStoreRegistry;

 public ConsumerApplication(
	 QueryableStoreRegistry queryableStoreRegistry) {
	this.queryableStoreRegistry = queryableStoreRegistry;
 }

 @StreamListener(SimpleKafkaStreamsProcessor.INPUT)
 public void process(KStream<String, DomainEvent> input) {

	KTable<String, Aggregate> table = input
		.groupBy((s, domainEvent) -> domainEvent.getBoardUuid(), Serialized.with(Serdes.String (), new JsonSerde<>(DomainEvent.class)))
		.aggregate(
			Aggregate::new,
			(key, value, aggregate) -> aggregate.addDomainEvent(value),
			Materialized.<String, Aggregate, KeyValueStore<Bytes, byte[]>>as("test-events-snapshots")
				.withKeySerde(Serdes.String())
				.withValueSerde(new JsonSerde<>(Aggregate.class))
		);
	table
		.toStream()
		.foreach((key, value) -> log.info(key + '=' + value));
 }

 @RequestMapping("/events/{key}")
 public Aggregate events(@PathVariable String key) {
	ReadOnlyKeyValueStore<String, Aggregate> queryableStoreType =
		queryableStoreRegistry.getQueryableStoreType("test-events-snapshots", QueryableStoreTypes.<String, Aggregate>keyValueStore());
	return queryableStoreType.get(key);
 }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Aggregate {

 public Aggregate addDomainEvent(DomainEvent event) {
	this.eventTypes.add(event.getEventType());
	return this;
 }

 private Collection<String> eventTypes = new ArrayList<>();
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class DomainEvent {
 private String eventType;
 private String boardUuid;
}

interface SimpleKafkaStreamsProcessor {

 String INPUT = "input";

 @Input(INPUT)
 KStream<?, ?> input();
}