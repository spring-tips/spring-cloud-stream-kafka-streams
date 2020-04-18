package com.example.analytics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class AnalyticsApplication {

		private static final String PAGE_COUNT_MV = "pcmv";

		public static void main(String[] args) {
				SpringApplication.run(AnalyticsApplication.class, args);
		}

		@Component
		public static class PageViewEventSource {

				@Bean
				public Supplier<PageViewEvent> pageViewEventSupplier() {
					List<String> names = Arrays.asList("mfisher", "dyser", "schacko", "abilan", "ozhurakousky", "grussell");
					List<String> pages = Arrays.asList("blog", "sitemap", "initializr", "news", "colophon", "about");
					return () -> {
						String rPage = pages.get(new Random().nextInt(pages.size()));
						String rName = pages.get(new Random().nextInt(names.size()));
						return new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);
					};
				}
		}

		@Component
		public static class PageViewEventProcessor {

				@Bean
				public Function<KStream<String, PageViewEvent>, KStream<String, Long>> process() {
					return e -> e.filter((key, value) -> value.getDuration() > 10)
							.map((key, value) -> new KeyValue<>(value.getPage(), "0"))
							.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
							.count(Materialized.as(AnalyticsApplication.PAGE_COUNT_MV))
							.toStream();
				}
		}

		@Component
		public static class PageCountSink {

				private final Log log = LogFactory.getLog(getClass());

				@Bean
				public Consumer<KTable<String, Long>> pageCount() {

					return counts -> counts
							.toStream()
							.foreach((key, value) -> log.info(key + "=" + value));
				}
		}

		@RestController
		public static class CountRestController {

				private final InteractiveQueryService interactiveQueryService;

				public CountRestController(InteractiveQueryService registry) {
						this.interactiveQueryService = registry;
				}

				@GetMapping("/counts")
				Map<String, Long> counts() {
						Map<String, Long> counts = new HashMap<>();
						ReadOnlyKeyValueStore<String, Long> queryableStoreType =
							this.interactiveQueryService.getQueryableStore(AnalyticsApplication.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore());
						KeyValueIterator<String, Long> all = queryableStoreType.all();
						while (all.hasNext()) {
								KeyValue<String, Long> value = all.next();
								counts.put(value.key, value.value);
						}
						return counts;
				}
		}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent {
	private String userId, page;
	private long duration;
}
