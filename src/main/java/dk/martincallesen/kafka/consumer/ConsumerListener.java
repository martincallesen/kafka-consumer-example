package dk.martincallesen.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerListener {
    void failedToReceivedRecord();
    void recordProcessed(ConsumerRecord<?, ?> consumerRecord);
}
