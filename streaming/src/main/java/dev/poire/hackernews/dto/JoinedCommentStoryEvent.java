package dev.poire.hackernews.dto;

import org.springframework.kafka.support.serializer.JsonSerde;

public record JoinedCommentStoryEvent(Comment comment, Story story) {
    public static JsonSerde<JoinedCommentStoryEvent> serde = new JsonSerde<>(JoinedCommentStoryEvent.class);
}
