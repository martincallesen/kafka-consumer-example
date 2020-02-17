package dk.martincallesen.kafka.serializer;

import dk.martincallesen.datamodel.event.Account;
import dk.martincallesen.datamodel.event.SpecificRecordAdapter;
import dk.martincallesen.datamodel.event.SpecificRecordDeserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;

public class KafkaDeserializer implements Deserializer<SpecificRecordAdapter> {
    private SpecificRecordDeserializer deserializer;

    public KafkaDeserializer() {
        this.deserializer = new SpecificRecordDeserializer<>(Account.class);
    }

    @Override
    public SpecificRecordAdapter deserialize(String topic, byte[] data) {
        try {
            return new SpecificRecordAdapter(deserializer.deserialize(topic, data));
        } catch (Exception ex) {
            throw new SerializationException(
                    "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
        }
    }
}
