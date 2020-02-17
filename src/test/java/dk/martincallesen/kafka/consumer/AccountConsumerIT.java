package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

@Import(ConsumerTestConfig.class)
@ExtendWith(SpringExtension.class)
@SpringBootTest(properties = {"spring.kafka.topic.boot=" + AccountConsumerIT.TOPIC})
@EmbeddedKafka(topics = AccountConsumerIT.TOPIC,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class AccountConsumerIT {
    public static final String TOPIC = "test-account-topic";

    @Autowired
    private KafkaTemplate<String, SpecificRecordAdapter> producer;

    @Autowired
    private SpecificRecordConsumer consumer;

    @Test
    void consumeAccountChange() throws InterruptedException {
        Account accountEvent = Account.newBuilder()
                .setName("MyAccount")
                .setReg(1234)
                .setNumber(1234567890)
                .build();
        consumer.setListener(ExpectRecord.received());
        producer.send(TOPIC, new SpecificRecordAdapter(accountEvent));
    }

    private static class ExpectRecord implements ConsumerListener {
        private final CountDownLatch latch;

        private ExpectRecord() throws InterruptedException {
            latch = new CountDownLatch(1);
            latch.await(10, TimeUnit.SECONDS);
        }

        public static ExpectRecord received() throws InterruptedException {
            return new ExpectRecord();
        }

        @Override
        public void failedToReceivedRecord() {
            fail("Record not received");
        }

        @Override
        public void recordProcessed(ConsumerRecord<?, ?> consumerRecord) {
            assertNotNull(consumerRecord, "Record not received");
            final Account accountRecord = ((SpecificRecordAdapter<Account>) consumerRecord.value()).getRecord();
            latch.countDown();
        }
    }
}
