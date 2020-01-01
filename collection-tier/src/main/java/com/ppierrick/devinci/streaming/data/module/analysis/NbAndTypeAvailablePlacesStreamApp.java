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
/**
 * This class is reponsible of acreating a new Topic that will contain:
 * The park name and the type of place (STANDARD-MOTOR_BIKE-ELECTRICAL_CAR) and the number of available
 * places per park name .
 */
public class NbAndTypeAvailablePlacesStreamApp {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "prakingstats-stream-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        NbAndTypeAvailablePlacesStreamApp nbAndTypeAvailablePlacesStreamApp = new NbAndTypeAvailablePlacesStreamApp();

        KafkaStreams streams = new KafkaStreams(nbAndTypeAvailablePlacesStreamApp.createTopology(), config);
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
        Serde<String> typeAndNbCounterPlace = Serdes.String();


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stats = builder.stream("parkings-saemes-stats-raw");
        KStream<String, String> parkingCountAndTypeStream = stats
                .selectKey((key, jsonRecordString) -> extractParkingName(jsonRecordString))
                .map((key, values) -> new KeyValue<>(key, extractPlaceTypeAndNumberFromJsonNode(values)));
        parkingCountAndTypeStream.to("parking-typeandnbfreeplaces-updates", Produced.with(stringSerde, typeAndNbCounterPlace));

        return builder.build();
    }

    private String extractPlaceTypeAndNumberFromJsonNode(String jsonRecordString) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsNode = jsonNode.get("fields");
        JsonNode countertype = fieldsNode.get("countertype");
        JsonNode counterfreeplaces = fieldsNode.get("counterfreeplaces");
        return "counterType : " + countertype.asText() + " , counterfreeplaces : " + counterfreeplaces;
    }


    private String extractParkingName(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsMode = jsonNode.get("fields");
        JsonNode parkingNameNode = fieldsMode.get("nom_parking");
        return parkingNameNode.asText();
    }


}
