package dev.poire.streaming.pipeline;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeyProviders;
import dev.poire.denormalize.schema.blake.Blake2bJoinKeyProvider;
import dev.poire.denormalize.transform.JoinKeyPartitioner;
import dev.poire.denormalize.transform.LeftKeyMapper;
import dev.poire.denormalize.transform.RightKeyMapper;
import dev.poire.streaming.dto.Comment;
import dev.poire.streaming.dto.JoinedCommentStoryEvent;
import dev.poire.streaming.dto.Story;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Lazy;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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
                .flatTransform(() -> new JoinValueTransformer<>(
                                Comment.serde,
                                Story.serde,
                                JoinedCommentStoryEvent::new,
                                (k, joined) -> joined.comment().id().toString(),
                                false,
                                false)
                        , "index")
                .to(topicJoined, Produced.with(Serdes.String(), JoinedCommentStoryEvent.serde));
    }

    /**
     * Processes an update from either side and does an update.
     *
     * @param <V>  The left-side value type.
     * @param <FV> The right-side value type.
     * @param <KR> The desired joined output key type.
     * @param <VR> The desired joined output value type.
     */
    static class JoinValueTransformer<V, FV, KR, VR> implements Transformer<JoinKey, Bytes, Iterable<KeyValue<KR, VR>>> {
        private ProcessorContext context;
        private final Serde<V> leftSerde;
        private final Serde<FV> rightSerde;
        private final ValueJoiner<V, FV, VR> valueJoiner;
        private final KeyValueMapper<JoinKey, VR, KR> keyMapper;

        private final Lazy<KeyValueStore<JoinKey, ValueAndTimestamp<Bytes>>> indexStore = Lazy.of(() -> context.getStateStore("index"));

        private final boolean leftOuter;
        private final boolean rightOuter;

        public JoinValueTransformer(
                Serde<V> leftSerde,
                Serde<FV> rightSerde,
                ValueJoiner<V, FV, VR> valueJoiner,
                KeyValueMapper<JoinKey, VR, KR> keyMapper,
                boolean leftOuter,
                boolean rightOuter
        ) {
            this.leftSerde = leftSerde;
            this.rightSerde = rightSerde;
            this.valueJoiner = valueJoiner;
            this.keyMapper = keyMapper;
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public Iterable<KeyValue<KR, VR>> transform(JoinKey joinKey, Bytes value) {
            var store = indexStore.get();
            log.info("Index {} store size {}", context.partition(), store.approximateNumEntries());

            if (joinKey.isLeft()) {
                // Find matching foreign key record in index, and attempt join.
                var matchIndexKey = joinKey.getRight();
                log.info("Received left-side {}, looking up indexed right using {}", joinKey, matchIndexKey);

                var right = store.get(matchIndexKey);

                if (right != null) {
                    var leftDeser = leftSerde.deserializer().deserialize(null, value.get());
                    var rightDeser = rightSerde.deserializer().deserialize(null, right.value().get());
                    var joined = valueJoiner.apply(leftDeser, rightDeser);
                    var key = keyMapper.apply(joinKey, joined);
                    return List.of(KeyValue.pair(key, joined));
                } else if (leftOuter) {
                    var leftDeser = leftSerde.deserializer().deserialize(null, value.get());
                    var joined = valueJoiner.apply(leftDeser, null);
                    var key = keyMapper.apply(joinKey, joined);
                    return List.of(KeyValue.pair(key, joined));
                } else {
                    return List.of();
                }
            } else {
                // Perform local prefix scan for all possible joins on this side.
                var prefix = joinKey.getPrefix();
                log.info("Received right-side {}, performing local prefix scan using {}", joinKey, Arrays.toString(prefix));

                final List<KeyValue<KR, VR>> matched = new LinkedList<>();
                // Lazily deserialize right value (always the same)
                var rightDeser = Lazy.of(() -> rightSerde.deserializer().deserialize(null, value.get()));

                store.prefixScan(prefix, new ByteArraySerializer()).forEachRemaining(bytesKeyValue -> {
                    // Ignore the right join key itself
                    if (bytesKeyValue.key.isLeft()) {
                        var match = bytesKeyValue.value.value();
                        var leftDeser = leftSerde.deserializer().deserialize(null, match.get());

                        var joined = valueJoiner.apply(leftDeser, rightDeser.get());
                        var key = keyMapper.apply(joinKey, joined);
                        matched.add(KeyValue.pair(key, joined));
                    }
                });

                if (!matched.isEmpty()) {
                    log.info("SCAN finished; emit {} join results", matched.size());
                }

                if (matched.isEmpty() && rightOuter) {
                    var joined = valueJoiner.apply(null, rightDeser.get());
                    var key = keyMapper.apply(joinKey, joined);
                    return List.of(KeyValue.pair(key, joined));
                }

                return matched;
            }
        }

        @Override
        public void close() {
        }
    }

}
