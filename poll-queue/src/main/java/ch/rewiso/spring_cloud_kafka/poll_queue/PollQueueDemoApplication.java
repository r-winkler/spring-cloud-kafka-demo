package ch.rewiso.spring_cloud_kafka.poll_queue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

@SpringBootApplication
@EnableScheduling
@EnableIntegration
public class PollQueueDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(PollQueueDemoApplication.class, args);
	}

	private static final String TOPIC = "my-topic";
	@Autowired
	private StreamBridge streamBridge;

	@Scheduled(cron = "*/2 * * * * *")
	public void sendMessage(){
		System.out.println("Send message.");
		streamBridge.send("producer-out-0", "Test from stream bridge");
	}

	// Option 1
	@Bean
	public IntegrationFlow flow() {

		return IntegrationFlows.from(Kafka.messageDrivenChannelAdapter(consumerFactory(),
								KafkaMessageDrivenChannelAdapter.ListenerMode.record, TOPIC))
				.channel("pollable")
				.get();
	}

	@Bean
	MessageChannel pollable() {
		return new QueueChannel();
	}

	@Bean
	public IntegrationFlow bridgeFlow() {
		return IntegrationFlows.from("pollable")
				.bridge(e -> e.poller(Pollers.fixedDelay(5000).maxMessagesPerPoll(10)))
				.channel("direct")
				.handle(it -> System.out.println("Received: " + new String((byte[]) it.getPayload(), StandardCharsets.UTF_8)))
				.get();
	}


//	@Bean
//	public KafkaMessageDrivenChannelAdapter<String, String>
//	adapter(KafkaMessageListenerContainer<String, String> container) {
//		KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter =
//				new KafkaMessageDrivenChannelAdapter<>(container, KafkaMessageDrivenChannelAdapter.ListenerMode.record);
//		kafkaMessageDrivenChannelAdapter.setOutputChannel(received());
//		return kafkaMessageDrivenChannelAdapter;
//	}
//
//	@Bean
//	public KafkaMessageListenerContainer<String, String> container() throws Exception {
//		ContainerProperties properties = new ContainerProperties(TOPIC);
//		// set more properties
//		return new KafkaMessageListenerContainer<>(consumerFactory(), properties);
//	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// set more properties
		return new DefaultKafkaConsumerFactory<>(props);
	}


}
