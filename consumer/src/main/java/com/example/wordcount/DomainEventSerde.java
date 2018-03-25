package com.example.wordcount;

import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */
public class DomainEventSerde extends JsonSerde<DomainEvent> {

	public DomainEventSerde() {
		super(DomainEvent.class);
	}
}
