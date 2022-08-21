package dev.poire.streaming.pipeline;

import dev.poire.streaming.dto.Comment;
import dev.poire.streaming.dto.JoinedCommentStoryEvent;
import dev.poire.streaming.dto.Story;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.state.Stores;
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
        // TODO: Replace with ElasticKeyValueStore
        var commentStore = Stores.inMemoryKeyValueStore("comments");
        var storiesStore = Stores.inMemoryKeyValueStore("stories");
        var joinedStore = Stores.inMemoryKeyValueStore("joined");

        var materializeComments = Materialized.<String, Comment>as(commentStore)
                .withLoggingDisabled()
                .withKeySerde(Serdes.String())
                .withValueSerde(Comment.serde);

        var materializeStories = Materialized.<String, Story>as(storiesStore)
                .withLoggingDisabled()
                .withKeySerde(Serdes.String())
                .withValueSerde(Story.serde);

        var materializeJoined = Materialized.<String, JoinedCommentStoryEvent>as(joinedStore)
                .withLoggingDisabled()
                .withKeySerde(Serdes.String())
                .withValueSerde(JoinedCommentStoryEvent.serde);

        var comments = builder.table(topicComments, materializeComments);
        var stories = builder.table(topicStories, materializeStories);

        // TODO: With KAFKA-10383 / KIP-718, we'll be able to pass a 'Materialized' for the internal join processor
        // In the mean-time, we have to create 3 topics for this join:
        // - '${app}-join-subscription-registration-topic'
        // - '${app}-join-subscription-response-topic'
        // - '${app}-join-subscription-store-changelog'
        comments.leftJoin(stories, comment -> comment.story().toString(), JoinedCommentStoryEvent::new,
                        TableJoined.as("joined"),
                        materializeJoined)
                .toStream()
                .to(topicJoined, Produced.with(Serdes.String(), JoinedCommentStoryEvent.serde));
    }
}
