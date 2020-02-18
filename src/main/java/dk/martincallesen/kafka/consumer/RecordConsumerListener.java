package dk.martincallesen.kafka.consumer;

import dk.martincallesen.datamodel.event.SpecificRecordAdapter;

public interface RecordConsumerListener {
    void recordProcessed(String key, SpecificRecordAdapter record);
}
