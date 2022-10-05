package ch.rewiso.basics_without_spring_cloud;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class BasicWithoutSpringCloudDemoApplication implements CommandLineRunner {

    @Value(value = "${kafka.topicName}")
    private String topicName;

    @Value(value = "${kafka.groupId}")
    private String groupId;

    public static void main(String[] args) {
        SpringApplication.run(BasicWithoutSpringCloudDemoApplication.class, args);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private void sendMessage(String msg) {
        kafkaTemplate.send(topicName, msg);
    }

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;


    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void listen(String message) {
        System.out.println("Received Message: " + message);
    }


    @Override
    public void run(String... args) throws Exception {
        sendMessage("Hello");
        sendMessage("ABC");
        sendMessage("Test successful!");

    }
}
