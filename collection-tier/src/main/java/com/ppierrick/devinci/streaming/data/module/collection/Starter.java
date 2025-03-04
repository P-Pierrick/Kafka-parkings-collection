package com.ppierrick.devinci.streaming.data.module.collection;

import java.util.Collections;

/**
 * @author Pierrick Pujol
 * @author HADHRI Anas
 */
public class Starter {

    public static void main(String[] args) throws InterruptedException {
        KafkaHandlerConfig config = buildConfig();
        KafkaPublisher publisher = new KafkaPublisher(config);
        Collector collector = new Collector(publisher);
        scheduleCollect(collector);
    }

    private static KafkaHandlerConfig buildConfig() {
        KafkaHandlerConfig config = new KafkaHandlerConfig();
        config.setBootstrapServers(Collections.singletonList("localhost:9092"));
        config.setTopicName("parkings-saemes-stats-raw");
        return config;
    }

    private static void scheduleCollect(Collector collector) throws InterruptedException {
        ScheduledExecutorRepeat executorRepeat = new ScheduledExecutorRepeat(collector, 10);
        executorRepeat.repeat();
    }
}
