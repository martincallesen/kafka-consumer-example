package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CustomerRecordConsumer {
    private RecordConsumerListener listener;
    private Logger logger = LoggerFactory.getLogger(CustomerRecordConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic.customer}", groupId="${spring.kafka.consumer.group.id}")
    public void processRecord(ConsumerRecord<String, SpecificRecordAdapter> consumerRecord){
        final SpecificRecordAdapter record = consumerRecord.value();
        logger.info("Received record: {} ", record);

        if(listener != null) {
            listener.recordProcessed(consumerRecord.key(), record);
        }
    }

    public void setListener(RecordConsumerListener consumerListener) {
        this.listener = consumerListener;
    }
}
