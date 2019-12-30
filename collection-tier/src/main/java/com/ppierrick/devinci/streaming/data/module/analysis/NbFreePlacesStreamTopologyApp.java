package com.ppierrick.devinci.streaming.data.module.analysis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Properties;

public class NbFreePlacesStreamTopologyApp {


    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "parkings-stats-stream-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        NbFreePlacesStreamTopologyApp dockCountApp = new NbFreePlacesStreamTopologyApp();

        KafkaStreams streams = new KafkaStreams(dockCountApp.createTopology(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while (true) {
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private Topology createTopology() {

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stats = builder.stream("parkings-saemes-stats-raw");
        KStream<String, Long> docksCountStream = stats
                .selectKey((key, jsonRecordString) -> extract_station_name(jsonRecordString))
                .map((key, value) -> new KeyValue<>(key, extract_nbfreePlaces(value)));

        docksCountStream.to("parking-nbfreeplaces-updates", Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    private String extract_station_name(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsMode = jsonNode.get("fields");

        JsonNode stationNameNode = fieldsMode.get("nom_parking");

        return stationNameNode.asText();
    }

    private Long extract_nbfreePlaces(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsMode = jsonNode.get("fields");

        JsonNode nbfreeedockNode = fieldsMode.get("counterfreeplaces");

        return Long.parseLong(nbfreeedockNode.asText());
    }
}
