package stocks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.extern.java.Log;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * two streams:
 * - ticker-prices (ticker, symbol)
 * - orders (userId, ticker, shares, when)
 * <p>
 * we'll create a third stream to show how many stocks a given user has purchased in, say, the last hour
 *
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */
@Log
@SpringBootApplication
@EnableBinding(StocksChannels.class)
public class StocksApplication {

 @Component
 public static class MyProducer implements ApplicationRunner {

	private final ScheduledExecutorService executors = Executors.newScheduledThreadPool(
		Runtime.getRuntime().availableProcessors());

	private final KafkaTemplate<String, Ticker> tickerKafkaTemplate;

	private final KafkaTemplate<String, Order> orderKafkaTemplate;

	private final BindingServiceProperties bindingServiceProperties;

	MyProducer(
		KafkaTemplate<String, Ticker> tickerKafkaTemplate,
		KafkaTemplate<String, Order> orderKafkaTemplate,
		BindingServiceProperties bindingServiceProperties) {
	 this.bindingServiceProperties = bindingServiceProperties;
	 this.tickerKafkaTemplate = tickerKafkaTemplate;
	 this.orderKafkaTemplate = orderKafkaTemplate;
	}

	private String topicForBinding(String bindingName) {
	 return this.bindingServiceProperties
		 .getBindingProperties(bindingName)
		 .getDestination();
	}

	@Override
	public void run(ApplicationArguments args) {
	 log.info("run(" + args.toString() + ")");

	 String orders = topicForBinding(StocksChannels.ORDERS_INBOUND);
	 String tickers = topicForBinding(StocksChannels.TICKERS_OUTBOUND);

	 List<String> users = Arrays.asList("josh", "jane", "rod", "mario", "andrew", "tasha");
	 List<String> stocks = Arrays.asList("VMW", "GOOG", "IBM", "MSFT", "ORCL", "RHT");

	 Runnable tickersRunnable = () -> {
		stocks
			.stream()
			.map(stock -> new Ticker(stock, (float) (Math.random() * 1000)))
			.forEach(ticker -> tickerKafkaTemplate.send(tickers, ticker));
	 };

/*
	 Runnable ordersRunnable = () -> {
		for (int i = 0; i < 10; i++) {
		 log.info("sending to " + orders + ".");
		 Order order = new Order(
			 random(users),
			 random(stocks),
			 new Date(),
			 (float) (Math.random() * 1000), (float) (Math.random() * 10));
		 log.info("about to send..");
		 orderKafkaTemplate.send(orders, order);
		 log.info("sent!");
		}
	 };*/

	 this.executors.scheduleWithFixedDelay(tickersRunnable, 1, 1, TimeUnit.SECONDS);
//	 this.executors.scheduleWithFixedDelay(ordersRunnable, 1, 1, TimeUnit.SECONDS);
	}

	private <T> T random(List<T> search) {
	 int index = Math.max(0, Math.min((int) (Math.random() * search.size()), search.size() - 1));
	 return search.get(index);
	}
 }

 @Log
 @Component
 public static class MyConsumer {

	@StreamListener
	public void process(
		@Input(StocksChannels.TICKERS_INBOUND) KStream<String, Ticker> orderKStream) {
	 orderKStream
		 .foreach((key, value) -> log.info(key + '=' + value));
	}
 }

 public static void main(String args[]) {
	SpringApplication.run(StocksApplication.class, args);
 }
}

@Data
class Ticker {
 private String symbol;
 private float price;

 @JsonCreator
 public Ticker(@JsonProperty("symbol") String s, @JsonProperty("price") float p) {
	this.symbol = s;
	this.price = p;
 }
}


@Data
class Order {

 private String ticker;
 private Date when;
 private float price;
 private float shares;
 private String userId;

 @JsonCreator
 Order(@JsonProperty("userId") String u,
			 @JsonProperty("ticker") String t,
			 @JsonProperty("when") Date w,
			 @JsonProperty("price") float p,
			 @JsonProperty("shares") float s) {
	this.shares = s;
	this.price = p;
	this.userId = u;
	this.when = w;
	this.ticker = t;
 }
}

interface StocksChannels {

 String ORDERS_OUTBOUND = "ordersOutbound";
 String TICKERS_OUTBOUND = "tickersOutbound";

 String TICKERS_INBOUND = "tickersInbound";
 String ORDERS_INBOUND = "ordersInbound";

 @Input(ORDERS_INBOUND) KStream<String, Order> ordersIn();
 @Output(ORDERS_OUTBOUND) KStream<String, Order> ordersOut();

 @Input(TICKERS_INBOUND) KStream<String, Ticker> tickersIn();
 @Input(TICKERS_OUTBOUND) KStream<String, Ticker> tickersOut();

}