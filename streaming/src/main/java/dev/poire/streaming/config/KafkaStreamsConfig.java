package dev.poire.streaming.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        return new KafkaStreamsConfiguration(Map.of(
                APPLICATION_ID_CONFIG, "app",
                BOOTSTRAP_SERVERS_CONFIG, "localhost:29092",
                TOPOLOGY_OPTIMIZATION_CONFIG, "all"
        ));
    }
}
