package dev.poire.hackernews.dto;

import org.springframework.kafka.support.serializer.JsonSerde;

public record Comment(String by, Long id, Long parent, String text, Long time, String type, Long story) {
    public static JsonSerde<Comment> serde = new JsonSerde<>(Comment.class);
}
