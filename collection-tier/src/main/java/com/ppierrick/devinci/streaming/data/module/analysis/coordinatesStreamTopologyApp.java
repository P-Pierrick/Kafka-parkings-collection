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

/**
 * @author Pierrick Pujol
 * @author HADHRI Anas
 */
public class coordinatesStreamTopologyApp {


    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "parkings-stats-stream-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        coordinatesStreamTopologyApp dockCountApp = new coordinatesStreamTopologyApp();

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
        Serde<String> stringSerde2 = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stats = builder.stream("parkings-saemes-stats-raw");
        KStream<String, String> docksCountStream = stats
                .selectKey((key, jsonRecordString) -> extract_station_name(jsonRecordString))
                .map((key, value) -> new KeyValue<>(key, extract_coordinates(value)));

        docksCountStream.to("parking-coordinates-updates", Produced.with(stringSerde, stringSerde2));

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

    private String extract_coordinates(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsMode = jsonNode.get("fields");

        JsonNode geo = fieldsMode.get("geo");

        String latitude = geo.get(0).asText();;

        String Longitude= geo.get(1).asText();;

        String coordinates = latitude + ";" + Longitude;

        return coordinates;
    }
}
