package dev.poire.streaming.pipeline;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeyProviders;
import dev.poire.denormalize.transform.JoinKeyPartitioner;
import dev.poire.denormalize.transform.JoinTransformer;
import dev.poire.streaming.dto.Comment;
import dev.poire.streaming.dto.JoinedCommentStoryEvent;
import dev.poire.streaming.dto.Story;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
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
        final var keyProvider = JoinKeyProviders.Blake2b(8, Serdes.String(), Serdes.String());

        // Left side of the join
        // Every time a LEFT is received, it will forward it to the INDEX topic, but re-keyed to include the foreign key,
        // and also, it will manually partition the output based on the foreign key only.
        builder.stream(topicComments, Consumed.with(Serdes.String(), Comment.serde))
                .selectKey(keyProvider.joinOn((k, comment) -> comment.story().toString()))
                .to(topicIndex, Produced.with(JoinKey.serde, Comment.serde).withStreamPartitioner(JoinKeyPartitioner.partitioner()));

        // Right side of the join
        // Every time a RIGHT is received, it will forward it to the INDEX topic, but re-keyed to have a NULL primary key,
        // and also, it will manually repartition the output based on the foreign key only.
        builder.stream(topicStories, Consumed.with(Serdes.String(), Story.serde))
                .selectKey(keyProvider.right())
                .to(topicIndex, Produced.with(JoinKey.serde, Story.serde).withStreamPartitioner(JoinKeyPartitioner.partitioner()));

        // TODO: In-memory is OK since the store will be rebuilt from changelog, but we should archive the index periodically
        // due to retention rules.
        var indexSupplier = Stores.inMemoryKeyValueStore("index");

        // The join.
        // On start-up it will start reading from the beginning to rebuild its internal store.
        // When it receives a non-null primary key, it tries to find 1 foreign document to join with.
        // When it receives a null primary key, it scans for all documents with the foreign key.
        // Make sure this topic has compaction enabled!
        var consumed = Consumed.with(JoinKey.serde, Serdes.Bytes());
        var materialized = Materialized.<JoinKey, Bytes>as(indexSupplier);
        builder.table(topicIndex, consumed, materialized)
                .toStream()
                .flatTransform(JoinTransformer.inner(
                                Comment.serde,
                                Story.serde,
                                JoinedCommentStoryEvent::new,
                                (k, joined) -> joined.comment().id().toString()),
                        "index")
                .to(topicJoined, Produced.with(Serdes.String(), JoinedCommentStoryEvent.serde));
    }
}
