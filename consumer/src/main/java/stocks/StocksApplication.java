package stocks;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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

	private final ScheduledExecutorService executors = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
	private final MessageChannel orders, tickers;

	MyProducer(StocksChannels channels) {
	 this.orders = channels.ordersOut();
	 this.tickers = channels.tickersOut();
	}

	@Override
	public void run(ApplicationArguments args) {

	 List<String> users = Arrays.asList("josh", "jane", "rod", "mario", "andrew", "tasha");
	 List<String> stocks = Arrays.asList("VMW", "GOOG", "IBM", "MSFT", "ORCL", "RHT");

	 Runnable tickersRunnable = () ->
		 stocks
			 .stream()
			 .map(stock -> new Ticker(stock, (float) (Math.random() * 1000)))
			 .map(ticker -> MessageBuilder.withPayload(ticker).build())
			 .forEach(this.tickers::send);

	 Runnable ordersRunnable = () ->
		 IntStream
			 .range(0, 10)
			 .mapToObj(indx -> new Order(
				 random(users),
				 random(stocks),
				 new Date(),
				 (float) (Math.random() * 1000),
				 (float) (Math.random() * 10)))
			 .map(o -> MessageBuilder.withPayload(o).build())
			 .forEach(this.orders::send);

	 this.executors.scheduleWithFixedDelay(tickersRunnable, 1, 1, TimeUnit.SECONDS);
	 this.executors.scheduleWithFixedDelay(ordersRunnable, 1, 1, TimeUnit.SECONDS);
	}

	private <T> T random(List<T> search) {
	 int index = Math.max(0, Math.min((int) (Math.random() * search.size()), search.size() - 1));
	 return search.get(index);
	}
 }


 @Log
 @Component
 public static class TickerConsumer {

	@StreamListener
	public void process(
		@Input(StocksChannels.TICKERS_INBOUND) KStream<String, Ticker> tickers,
		@Input(StocksChannels.ORDERS_INBOUND) KStream<String, Order> orders) {

	 orders.foreach((key, value) -> log.info(key + '=' + value));
	 tickers.foreach((k, v) -> log.info(k + '=' + v));
	}
 }

 /*@Log
 @Component*/
 public static class OrderConsumer {

	@StreamListener
	public void orders(@Input(StocksChannels.ORDERS_INBOUND) KStream<String, Order> orderKStream) {
	 orderKStream
		 .foreach((key, value) -> log.info(key + "=" + value));
	}

 }

 public static void main(String args[]) {
	SpringApplication.run(StocksApplication.class, args);
 }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class Ticker {
 private String symbol;
 private float price;
}

@Data
@NoArgsConstructor
class Order {

 private String userId;
 private String ticker;
 private Date when;
 private float price;
 private float shares;

 public Order(String userId,
							String ticker,
							Date when,
							float price,
							float shares) {
	this.userId = userId;
	this.ticker = ticker;
	this.when = when;
	this.price = price;
	this.shares = shares;
 }
}

interface StocksChannels {

 String ORDERS_OUTBOUND = "ordersOutbound";
 String TICKERS_OUTBOUND = "tickersOutbound";

 String TICKERS_INBOUND = "tickersInbound";
 String ORDERS_INBOUND = "ordersInbound";

 @Input(ORDERS_INBOUND)
 KStream<String, Order> ordersIn();

 @Input(TICKERS_INBOUND)
 KStream<String, Ticker> tickersIn();

 @Output(TICKERS_OUTBOUND)
 MessageChannel tickersOut();

 @Output(ORDERS_OUTBOUND)
 MessageChannel ordersOut();
}