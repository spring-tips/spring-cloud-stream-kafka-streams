package com.example.analytics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


interface AnalyticsBinding {

		String PAGE_VIEWS_OUT = "pvout";
		String PAGE_VIEWS_IN = "pvin";
		String PAGE_COUNT_MV = "pcmv";
		String PAGE_COUNT_OUT = "pcout";
		String PAGE_COUNT_IN = "pcin";

		// page views
		@Input(PAGE_VIEWS_IN)
		KStream<String, PageViewEvent> pageViewsIn();

		@Output(PAGE_VIEWS_OUT)
		MessageChannel pageViewsOut();

		// page cocunts
		@Output(PAGE_COUNT_OUT)
		KStream<String, Long> pageCountOut();

		@Input(PAGE_COUNT_IN)
		KTable<String, Long> pageCountIn();
}

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
public class AnalyticsApplication {

		public static void main(String[] args) {
				SpringApplication.run(AnalyticsApplication.class, args);
		}

		@Component
		public static class PageViewEventSource implements ApplicationRunner {

				private final MessageChannel pageViewsOut;
				private final Log log = LogFactory.getLog(getClass());

				public PageViewEventSource(AnalyticsBinding binding) {
						this.pageViewsOut = binding.pageViewsOut();
				}

				@Override
				public void run(ApplicationArguments args) throws Exception {
						List<String> names = Arrays.asList("mfisher", "dyser", "schacko", "abilan", "ozhurakousky", "grussell");
						List<String> pages = Arrays.asList("blog", "sitemap", "initializr", "news", "colophon", "about");
						Runnable runnable = () -> {
								String rPage = pages.get(new Random().nextInt(pages.size()));
								String rName = pages.get(new Random().nextInt(names.size()));
								PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);
								Message<PageViewEvent> message = MessageBuilder
									.withPayload(pageViewEvent)
									.setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
									.build();
								try {
										this.pageViewsOut.send(message);
										log.info("sent " + message.toString());
								}
								catch (Exception e) {
										log.error(e);
								}
						};
						Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
				}
		}

		@Component
		public static class PageViewEventProcessor {

				@StreamListener
				@SendTo(AnalyticsBinding.PAGE_COUNT_OUT)
				public KStream<String, Long> process(@Input(AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, PageViewEvent> events) {
						return events
							.filter((key, value) -> value.getDuration() > 10)
							.map((key, value) -> new KeyValue<>(value.getPage(), "0"))
							.groupByKey()
							.count(Materialized.as(AnalyticsBinding.PAGE_COUNT_MV))
							.toStream();
				}
		}

		@Component
		public static class PageCountSink {

				private final Log log = LogFactory.getLog(getClass());

				@StreamListener
				public void pageCount(@Input((AnalyticsBinding.PAGE_COUNT_IN)) KTable<String, Long> counts) {


						counts
							.toStream()
							.foreach((key, value) -> log.info(key + "=" + value));
				}
		}

		@RestController
		public static class CountRestController {

				private final QueryableStoreRegistry registry;

				public CountRestController(QueryableStoreRegistry registry) {
						this.registry = registry;
				}

				@GetMapping("/counts")
				Map<String, Long> counts() {
						Map<String, Long> counts = new HashMap<>();
						ReadOnlyKeyValueStore<String, Long> queryableStoreType =
							this.registry.getQueryableStoreType(AnalyticsBinding.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore());
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