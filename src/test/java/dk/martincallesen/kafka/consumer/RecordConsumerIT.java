package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@Import(ProducerTestConfig.class)
@ExtendWith(SpringExtension.class)
@SpringBootTest(properties = {"spring.kafka.topic.boot=" + RecordConsumerIT.TOPIC})
@EmbeddedKafka(topics = RecordConsumerIT.TOPIC,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class RecordConsumerIT implements RecordConsumerListener {
    public static final String TOPIC = "test-account-topic";
    private CountDownLatch latch;
    private SpecificRecordAdapter receivedRecord;

    @Autowired
    private KafkaTemplate<String, SpecificRecordAdapter> producer;

    @Autowired
    private RecordConsumer consumer;

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
        final SpecificRecordAdapter expectedRecord = new SpecificRecordAdapter(accountChange);
        producer.send(TOPIC, expectedRecord);
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(expectedRecord, receivedRecord, "Record received");
    }

    @Override
    public void recordProcessed(String key, SpecificRecordAdapter record) {
        receivedRecord = record;
        latch.countDown();
    }
}
