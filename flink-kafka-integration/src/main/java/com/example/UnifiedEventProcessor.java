package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;

public class UnifiedEventProcessor {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set a parallelism for better debugging
        env.setParallelism(1);

        // Kafka properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProps.setProperty("group.id", "event-processor");
        kafkaProps.setProperty("auto.offset.reset", "earliest"); // Ensure processing from the start

        // Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            "event-topic",
            new SimpleStringSchema(),
            kafkaProps
        );

        // Data stream
        DataStream<String> eventStream = env.addSource(kafkaConsumer).name("Event Stream");

        // Process events
        eventStream
            .flatMap((String eventJson, Collector<String> out) -> {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    // Parse JSON event
                    Map<String, Object> event = objectMapper.readValue(eventJson, Map.class);

                    // Extract event type and process accordingly
                    String eventType = (String) event.get("event_type");
                    if (eventType == null) {
                        out.collect("Error: Missing 'event_type' in event: " + eventJson);
                        return;
                    }

                    switch (eventType) {
                        case "visit":
                            processVisitEvent(event, out);
                            break;
                        case "scroll":
                            processScrollEvent(event, out);
                            break;
                        case "stay":
                            processStayEvent(event, out);
                            break;
                        case "trigger":
                            processTriggerEvent(event, out);
                            break;
                        default:
                            out.collect("Unknown event type: " + eventType + ", Event: " + eventJson);
                            break;
                    }
                } catch (Exception e) {
                    out.collect("Error processing event: " + e.getMessage() + ", Event: " + eventJson);
                }
            })
            .returns(TypeInformation.of(String.class)) // Ensure Flink knows the output type
            .name("Event Processor")
            .print("Event Output");

        // Execute Flink job
        env.execute("Unified Event Stream Processing");
    }

    private static void processVisitEvent(Map<String, Object> event, Collector<String> out) {
        // Handle visit event
        out.collect("Processed visit event: " + event);
    }

    private static void processScrollEvent(Map<String, Object> event, Collector<String> out) {
        // Handle scroll event
        out.collect("Processed scroll event: " + event);
    }

    private static void processStayEvent(Map<String, Object> event, Collector<String> out) {
        // Handle stay event
        out.collect("Processed stay event: " + event);
    }

    private static void processTriggerEvent(Map<String, Object> event, Collector<String> out) {
        // Handle trigger event and perform backtracking
        out.collect("Processed trigger event: " + event);
        // Add your backtracking logic here
    }
}