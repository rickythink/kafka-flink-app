package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class UnifiedEventProcessor {
    private static final long EXPIRY_THRESHOLD_MS = 24 * 60 * 60 * 1000; // 24 hours in milliseconds

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProps.setProperty("group.id", "event-processor");
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            "event-topic",
            new SimpleStringSchema(),
            kafkaProps
        );

        DataStream<String> eventStream = env.addSource(kafkaConsumer).name("Event Stream");

        eventStream
            .keyBy(eventJson -> {
                try {
                    Map<String, Object> event = new ObjectMapper().readValue(eventJson, Map.class);
                    return event.get("user_id").toString();
                } catch (Exception e) {
                    return "unknown";
                }
            })
            .process(new UserEventProcessor())
            .name("User Event Processor")
            .print();

        env.execute("Unified Event Stream Processing");
    }

    public static class UserEventProcessor extends KeyedProcessFunction<String, String, String> {
        private transient MapState<String, Tuple> urlVisitCounts;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Tuple> descriptor = new MapStateDescriptor<>(
                "urlVisitCounts",
                TypeInformation.of(String.class),
                TypeInformation.of(Tuple.class)
            );
            urlVisitCounts = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(String eventJson, Context ctx, Collector<String> out) throws Exception {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> event = objectMapper.readValue(eventJson, Map.class);

            String eventType = (String) event.get("event_type");
            String url = (String) event.get("url");
            long eventTime = (long) event.get("time");

            if ("visit".equals(eventType)) {
                processVisitEvent(url, eventTime, ctx, out);
            } else if ("trigger".equals(eventType)) {
                processTriggerEvent(url, eventTime, ctx, out);
            }

            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 60 * 60 * 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long currentTime = System.currentTimeMillis();
            Iterator<Map.Entry<String, Tuple>> iterator = urlVisitCounts.entries().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, Tuple> entry = iterator.next();
                if (currentTime - entry.getValue().lastUpdatedTime > EXPIRY_THRESHOLD_MS) {
                    iterator.remove();
                }
            }

            out.collect("State cleanup complete for user: " + ctx.getCurrentKey());
        }

        private void processVisitEvent(String url, long eventTime, Context ctx, Collector<String> out) throws Exception {
            Tuple tuple = urlVisitCounts.get(url);
            if (tuple == null) {
                tuple = new Tuple(0, 0);
            }
            tuple.count += 1;
            tuple.lastUpdatedTime = eventTime;
            urlVisitCounts.put(url, tuple);

            out.collect("User: " + ctx.getCurrentKey() + ", URL: " + url + ", Visit Count: " + tuple.count);
        }

        private void processTriggerEvent(String url, long eventTime, Context ctx, Collector<String> out) throws Exception {
            Tuple tuple = urlVisitCounts.get(url);
            int visitCount = tuple == null ? 0 : tuple.count;

            long currentTime = System.currentTimeMillis();
            long latency = currentTime - eventTime;

            out.collect("Processed trigger event for User: " + ctx.getCurrentKey() +
                        ", URL: " + url +
                        ", Visit Count: " + visitCount +
                        ", Latency: " + latency + " ms");
        }
    }

    public static class Tuple {
        public int count;
        public long lastUpdatedTime;

        public Tuple(int count, long lastUpdatedTime) {
            this.count = count;
            this.lastUpdatedTime = lastUpdatedTime;
        }
    }
}