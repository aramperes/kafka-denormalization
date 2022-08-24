package dev.poire.denormalize.dsl;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeySchema;
import dev.poire.denormalize.transform.JoinTransformer;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;

import java.util.function.Function;

import static dev.poire.denormalize.transform.JoinKeyPartitioner.partitioner;

@Data
@Builder
public class StreamDenormalize<LK, LV, RK, RV, JK, JV> {
    private final String leftTopic;
    private final String rightTopic;

    private final Serde<LV> leftSerde;
    private final Serde<RV> rightSerde;

    private final JoinKeySchema<LK, RK> keySchema;
    private final KeyValueBytesStoreSupplier indexStore;
    private final String indexTopic;
    private Function<LV, RK> joinOn;
    private Function<LK, RK> joinOnKeys;
    private final ValueJoiner<LV, RV, JV> joiner;
    private final KeyValueMapper<JoinKey, JV, JK> keyMapper;

    public KStream<JK, JV> innerJoin(StreamsBuilder builder) {
        if (joinOn != null && joinOnKeys != null)
            throw new IllegalArgumentException("Must provide at most one of: joinOn, joinOnKeys");

        // Left side of the join
        // Every time a LEFT is received, it will forward it to the INDEX topic, but re-keyed to include the foreign key,
        // and also, it will manually partition the output based on the foreign key only.
        if (joinOn != null) {
            builder.stream(leftTopic, Consumed.with(keySchema.leftSerde(), leftSerde))
                    .selectKey(keySchema.joinOn(joinOn))
                    .to(indexTopic, Produced.with(JoinKey.serde, leftSerde).withStreamPartitioner(partitioner()));
        } else if (joinOnKeys != null) {
            builder.stream(leftTopic, Consumed.with(keySchema.leftSerde(), Serdes.ByteArray()))
                    .selectKey((k, v) -> keySchema.generateJoinKey(joinOnKeys.apply(k), k))
                    .to(indexTopic, Produced.with(JoinKey.serde, Serdes.ByteArray()).withStreamPartitioner(partitioner()));
        } else {
            throw new IllegalArgumentException("Must provide one of: joinOn, joinOnKeys");
        }

        // Right side of the join
        // Every time a RIGHT is received, it will forward it to the INDEX topic, but re-keyed to have a NULL primary key,
        // and also, it will manually repartition the output based on the foreign key only.
        builder.stream(rightTopic, Consumed.with(keySchema.rightSerde(), Serdes.ByteArray()))
                .selectKey(keySchema.right())
                .to(indexTopic, Produced.with(JoinKey.serde, Serdes.ByteArray()).withStreamPartitioner(partitioner()));

        // The join.
        // On start-up it will start reading from the beginning to rebuild its internal store.
        // When it receives a non-null primary key, it tries to find 1 foreign document to join with.
        // When it receives a null primary key, it scans for all documents with the foreign key.
        // Make sure this topic has compaction enabled!
        var consumed = Consumed.with(JoinKey.serde, Serdes.Bytes());
        var materialized = Materialized.<JoinKey, Bytes>as(indexStore);
        return builder.table(indexTopic, consumed, materialized)
                .toStream()
                .flatTransform(JoinTransformer.inner(
                                leftSerde,
                                rightSerde,
                                joiner,
                                keyMapper),
                        indexStore.name());
    }
}
