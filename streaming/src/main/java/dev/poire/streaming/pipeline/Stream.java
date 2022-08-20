package dev.poire.streaming.pipeline;

import dev.poire.streaming.dto.Comment;
import dev.poire.streaming.dto.JoinedCommentStoryEvent;
import dev.poire.streaming.dto.Story;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Stream {

    @Value("${topics.stories}")
    private String topicStories;

    @Value("${topics.comments}")
    private String topicComments;

    @Value("${topics.joined}")
    private String topicJoined;

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        var comments = builder.table(topicComments, Materialized.<String, Comment, KeyValueStore<Bytes, byte[]>>as("comments")
                .withKeySerde(Serdes.String()).withValueSerde(Comment.serde).withLoggingDisabled());
        var stories = builder.table(topicStories, Materialized.<String, Story, KeyValueStore<Bytes, byte[]>>as("stories")
                .withKeySerde(Serdes.String()).withValueSerde(Story.serde).withLoggingDisabled());

        comments.leftJoin(stories, comment -> comment.story().toString(), JoinedCommentStoryEvent::new,
                        TableJoined.as("joined"),
                        Materialized.<String, JoinedCommentStoryEvent, KeyValueStore<Bytes, byte[]>>as("joined")
                                .withKeySerde(Serdes.String()).withValueSerde(JoinedCommentStoryEvent.serde))
                .toStream()
                .to(topicJoined, Produced.with(Serdes.String(), JoinedCommentStoryEvent.serde));
    }
}
