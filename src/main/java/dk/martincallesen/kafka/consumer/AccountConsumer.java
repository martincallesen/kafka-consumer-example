package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AccountConsumer {
    private AccountConsumerListener listener;
    private Logger logger = LoggerFactory.getLogger(AccountConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic.boot}", groupId="${spring.kafka.consumer.group.id}")
    public void processMessage(ConsumerRecord<String, SpecificRecordAdapter> consumerRecord){
        Account account = (Account) consumerRecord.value().getRecord();
        logger.info("Received message: {} ", account);

        if(listener != null) {
            listener.recordProcessed(consumerRecord.key(), account);
        }
    }

    public void setListener(AccountConsumerListener consumerListener) {
        this.listener = consumerListener;
    }
}
