package dev.poire.streaming.pipeline;

import dev.poire.denormalize.dsl.StreamDenormalize;
import dev.poire.denormalize.schema.JoinKeySchemas;
import dev.poire.streaming.dto.Comment;
import dev.poire.streaming.dto.JoinedCommentStoryEvent;
import dev.poire.streaming.dto.Story;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;
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

    @Value("${topics.index}")
    private String topicIndex;

    @Value("${topics.joined}")
    private String topicJoined;

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        // TODO: In-memory is OK since the store will be rebuilt from changelog, but we should archive the index periodically
        // due to retention rules.
        var indexSupplier = Stores.inMemoryKeyValueStore("index");

        // Denormalizes (joins) the comments (left) and stories (right) topics.
        // This is an inner join; values are only emitted if both sides have a match.
        StreamDenormalize.<String, Comment, String, Story, String, JoinedCommentStoryEvent>builder()
                .keySchema(JoinKeySchemas.Blake2b(8, Serdes.String(), Serdes.String()))
                .indexTopic(topicIndex)
                    .indexStore(indexSupplier)
                .leftTopic(topicComments)
                    .leftSerde(Comment.serde)
                .rightTopic(topicStories)
                    .rightSerde(Story.serde)
                .joinOn(comment -> comment.story().toString())
                    .joiner(JoinedCommentStoryEvent::new)
                    .keyMapper((k, joined) -> joined.comment().id().toString())
                .build()
                .innerJoin(builder)
                    .to(topicJoined, Produced.with(Serdes.String(), JoinedCommentStoryEvent.serde));
    }
}
