package io.confluent.developer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class KafkaProducerApplication {
    private final Producer<String, String> producer;
    private final String outTopic;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "This program takes two arguments: the path to an environment configuration file and" +
                            "the path to the file with records to send");
        }
        final Properties props = loadProperties(args[0]);
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        KafkaProducerApplication producerApp = new KafkaProducerApplication(producer, props.getProperty("output.topic.name"));
        String filePath = args[1];
        try {
            List<String> linesToProduce = Files.readAllLines(Paths.get(filePath));
            List<Future<RecordMetadata>> metadata = linesToProduce.stream()
                    .filter(l -> !l.trim().isEmpty())
                    .map(producerApp::produce)
                    .collect(Collectors.toList());
            producerApp.printMetadata(metadata, filePath);
        } catch (IOException e) {
            System.err.printf("Error reading file %s due to %s %n", filePath, e);
        } finally {
            producerApp.shutdown();
        }
    }

    public void printMetadata(final Collection<Future<RecordMetadata>> metadata, final String fileName) {
        log.info("Offsets and timestamps committed in batch from " + fileName);
        metadata.forEach(m -> {
            try {
                final RecordMetadata recordMetadata = m.get();
                log.info("Record written to offset " + recordMetadata.offset() + " timestamp " + recordMetadata.timestamp());
            } catch (InterruptedException e) {
                log.error(e.toString());
            } catch (ExecutionException e) {
                log.error(e.toString());
            }
        });

    }

    public void shutdown() {
        producer.close();
    }

    private Future<RecordMetadata> produce(final String message) {
        final String[] parts = message.split("-");
        final String key, value;
        if (parts.length > 1) {
            key = parts[0];
            value = parts[1];
        } else {
            key = null;
            value = parts[0];
        }
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outTopic, key, value);
        return producer.send(producerRecord);
    }

    private static Properties loadProperties(String fileName) throws IOException {
        Properties props = new Properties();
        try (FileInputStream inputStream = new FileInputStream(fileName)) {
            props.load(inputStream);
        }
        return props;
    }
}
