package dev.poire.hackernews.dto;

import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;

public record Story(String by, Long descendants, Long id, List<Long> kids, Long score, Long time, String title,
                    String type, String url, String text) {
    public static JsonSerde<Story> serde = new JsonSerde<>(Story.class);
}
