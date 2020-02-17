package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.Account;

public interface AccountConsumerListener {
    void recordProcessed(String key, Account account);
}
