package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SpecificRecordConsumer {
    private ConsumerListener listener;
    private Logger logger = LoggerFactory.getLogger(SpecificRecordConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic.boot}", groupId="${spring.kafka.consumer.group.id}")
    public void processMessage(ConsumerRecord<String, SpecificRecordAdapter<Account>> consumerRecord){
        logger.info("Received message: {} ", consumerRecord.value().getRecord());

        if(listener != null) {
            listener.recordProcessed(consumerRecord);
        }
    }

    public void setListener(ConsumerListener consumerListener) {
        this.listener = consumerListener;
    }
}
