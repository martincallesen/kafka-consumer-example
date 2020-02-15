package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import dk.martincallesen.kafka.serializer.KafkaDeserializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaDeserializer.class);

        return props;
    }

    @Bean
    public ProducerFactory<String, SpecificRecordAdapter> consumerFactory() {
        return new DefaultKafkaProducerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaTemplate<String, SpecificRecordAdapter> kafkaTemplate() {
        return new KafkaTemplate<>(consumerFactory());
    }

    @Bean
    public NewTopic accountTopic() {
        return TopicBuilder.name(SpecificRecordConsumer.TOPIC)
                .partitions(10)
                .replicas(1)
                .compact()
                .build();
    }
}
