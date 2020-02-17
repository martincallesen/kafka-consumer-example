package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerListener {
    void recordProcessed(ConsumerRecord<String, SpecificRecordAdapter<Account>> consumerRecord);
}
