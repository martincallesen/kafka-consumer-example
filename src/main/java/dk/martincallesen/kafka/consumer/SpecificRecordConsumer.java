package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SpecificRecordConsumer {
    private ConsumerListener listener;

    @KafkaListener(topics = "${spring.kafka.topic.boot}", groupId="${spring.kafka.consumer.group.id}")
    public void processMessage(ConsumerRecord<String, SpecificRecordAdapter<Account>> consumerRecord){
        if(listener != null) {
            listener.recordProcessed(consumerRecord);
        }
    }

    public void setListener(ConsumerListener consumerListener) {
        this.listener = consumerListener;
    }
}
