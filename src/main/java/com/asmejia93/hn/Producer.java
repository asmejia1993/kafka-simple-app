package com.asmejia93.hn;

import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(HelloKafka.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            HashMap<String, String> characters = new HashMap<String, String>();
            characters.put("hobbits", "Frodo");
            characters.put("elves", "Galadriel");
            characters.put("humans", "Éowyn");

            characters.forEach((key, value) -> {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("lotr_characters", key, value);
                producer.send(producerRecord, (recordMetadata, err) -> {
                    if (err == null) {
                        log.info("Message received. \n" +
                                "topic [" + recordMetadata.topic() + "]\n" +
                                "partition [" + recordMetadata.partition() + "]\n" +
                                "offset [" + recordMetadata.offset() + "]\n" +
                                "timestamp [" + recordMetadata.timestamp() + "]");
                    } else {
                        log.error("An error occurred while producing messages", err);
                    }
                });
            });
            producer.close();
            log.info("producer is now closed");
        }
    }
}
