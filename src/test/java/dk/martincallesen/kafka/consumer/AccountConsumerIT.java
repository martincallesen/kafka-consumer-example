package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Import(ProducerTestConfig.class)
@ExtendWith(SpringExtension.class)
@SpringBootTest(properties = {"spring.kafka.topic.boot=" + AccountConsumerIT.TOPIC})
@EmbeddedKafka(topics = AccountConsumerIT.TOPIC,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class AccountConsumerIT implements AccountConsumerListener {
    public static final String TOPIC = "test-account-topic";
    private CountDownLatch latch;
    private Account receivedAccountRecord;

    @Autowired
    private KafkaTemplate<String, SpecificRecordAdapter<Account>> producer;

    @Autowired
    private AccountConsumer consumer;

    @BeforeEach
    void setupConsumer() {
        latch = new CountDownLatch(1);
        consumer.setListener(this);
    }

    @Test
    void consumeAccountChange() throws InterruptedException {
        Account accountChange = Account.newBuilder()
                .setName("MyAccount")
                .setReg(1234)
                .setNumber(1234567890)
                .build();
        producer.send(TOPIC, new SpecificRecordAdapter<>(accountChange));
        latch.await(10, TimeUnit.SECONDS);
        Assertions.assertEquals(accountChange, receivedAccountRecord, "Record received");
    }

    @Override
    public void recordProcessed(String key, Account consumerRecord) {
        receivedAccountRecord = consumerRecord;
        latch.countDown();
    }
}
