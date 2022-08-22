package dev.poire.denormalize.transform;

import dev.poire.denormalize.schema.JoinKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.data.util.Lazy;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


/**
 * Processes an update from either side of the join, and emits the joined values.
 *
 * @param <V>  The left-side value type.
 * @param <FV> The right-side value type.
 * @param <KR> The desired joined output key type.
 * @param <VR> The desired joined output value type.
 */
@Slf4j
public class JoinTransformer<V, FV, KR, VR> implements Transformer<JoinKey, Bytes, Iterable<KeyValue<KR, VR>>> {
    private ProcessorContext context;
    private final Serde<V> leftSerde;
    private final Serde<FV> rightSerde;
    private final ValueJoiner<V, FV, VR> valueJoiner;
    private final KeyValueMapper<JoinKey, VR, KR> keyMapper;

    private final Lazy<KeyValueStore<JoinKey, ValueAndTimestamp<Bytes>>> indexStore = Lazy.of(() -> context.getStateStore("index"));

    private final boolean leftOuter;
    private final boolean rightOuter;

    /**
     * Processes an update from either side of the join, and emits the joined values.
     *
     * @param leftSerde   The serde for the left-side values.
     * @param rightSerde  The serde for the right-side values.
     * @param valueJoiner Function that combines the two sides into the desired output.
     * @param keyMapper   Function used to produce the output key.
     * @param leftOuter   Whether this is a left-outer-join, i.e., unmatched left updates will be emitted with a null right side.
     * @param rightOuter  Whether this is a right-outer-join, i.e., unmatched right updates will be emitted with a null right side.
     */
    public JoinTransformer(
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

    public static <V, FV, KR, VR> TransformerSupplier<JoinKey, Bytes, Iterable<KeyValue<KR, VR>>> inner(
            Serde<V> leftSerde,
            Serde<FV> rightSerde,
            ValueJoiner<V, FV, VR> valueJoiner,
            KeyValueMapper<JoinKey, VR, KR> keyMapper) {
        return () -> new JoinTransformer<>(
                leftSerde,
                rightSerde,
                valueJoiner,
                keyMapper,
                false,
                false
        );
    }

    public static <V, FV, KR, VR> TransformerSupplier<JoinKey, Bytes, Iterable<KeyValue<KR, VR>>> leftOuter(
            Serde<V> leftSerde,
            Serde<FV> rightSerde,
            ValueJoiner<V, FV, VR> valueJoiner,
            KeyValueMapper<JoinKey, VR, KR> keyMapper) {
        return () -> new JoinTransformer<>(
                leftSerde,
                rightSerde,
                valueJoiner,
                keyMapper,
                true,
                false
        );
    }

    public static <V, FV, KR, VR> TransformerSupplier<JoinKey, Bytes, Iterable<KeyValue<KR, VR>>> rightOuter(
            Serde<V> leftSerde,
            Serde<FV> rightSerde,
            ValueJoiner<V, FV, VR> valueJoiner,
            KeyValueMapper<JoinKey, VR, KR> keyMapper) {
        return () -> new JoinTransformer<>(
                leftSerde,
                rightSerde,
                valueJoiner,
                keyMapper,
                false,
                true
        );
    }

    public static <V, FV, KR, VR> TransformerSupplier<JoinKey, Bytes, Iterable<KeyValue<KR, VR>>> fullOuter(
            Serde<V> leftSerde,
            Serde<FV> rightSerde,
            ValueJoiner<V, FV, VR> valueJoiner,
            KeyValueMapper<JoinKey, VR, KR> keyMapper) {
        return () -> new JoinTransformer<>(
                leftSerde,
                rightSerde,
                valueJoiner,
                keyMapper,
                true,
                true
        );
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public Iterable<KeyValue<KR, VR>> transform(JoinKey joinKey, Bytes value) {
        var store = indexStore.get();
        log.trace("Index {} store size {}", context.partition(), store.approximateNumEntries());

        if (joinKey.isLeft()) {
            // Find matching foreign key record in index, and attempt join.
            var matchIndexKey = joinKey.getRight();
            log.debug("Received left-side {}, looking up indexed right using {}", joinKey, matchIndexKey);

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
            log.debug("Received right-side {}, performing local prefix scan using {}", joinKey, Arrays.toString(prefix));

            final List<KeyValue<KR, VR>> matched = new LinkedList<>();
            // Lazily deserialize right value (always the same)
            var rightDeser = Lazy.of(() -> rightSerde.deserializer().deserialize(null, value.get()));

            store.prefixScan(prefix, new ByteArraySerializer()).forEachRemaining(scanned -> {
                // Ignore the right join key itself
                if (scanned.key.isLeft()) {
                    var match = scanned.value.value();
                    var leftDeser = leftSerde.deserializer().deserialize(null, match.get());

                    var joined = valueJoiner.apply(leftDeser, rightDeser.get());
                    var key = keyMapper.apply(scanned.key, joined);
                    matched.add(KeyValue.pair(key, joined));
                }
            });

            if (!matched.isEmpty()) {
                log.debug("SCAN finished; emit {} join results", matched.size());
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
