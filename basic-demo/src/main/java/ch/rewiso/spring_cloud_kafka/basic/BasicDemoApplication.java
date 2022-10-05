package ch.rewiso.spring_cloud_kafka.basic;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

@SpringBootApplication
@EnableScheduling
public class BasicDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(BasicDemoApplication.class, args);
    }

//    @Bean
//    public Supplier<String> producer() {
//        return () -> "This is a test";
//    }

    @Bean
    public Consumer<Message> consumer() {

        return message -> {
            if(Math.random()*10 > 0.001) {
                System.out.println("Received: " + new String((byte[]) message.getPayload(), StandardCharsets.UTF_8));
                throw new RuntimeException();
            }
            System.out.println("Consumed: " + new String((byte[]) message.getPayload(), StandardCharsets.UTF_8));
        };
    }

    @Autowired
    private StreamBridge streamBridge;

    @Scheduled(cron = "*/2 * * * * *")
    public void sendMessage() {
        var str = "Send from stream bridge: " + UUID.randomUUID();
        System.out.println(str);
        streamBridge.send("producer-out-0", str);
    }

}
