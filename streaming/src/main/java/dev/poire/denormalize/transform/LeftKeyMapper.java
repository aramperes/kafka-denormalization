package dev.poire.denormalize.transform;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeySchema;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.function.Function;

public class LeftKeyMapper<K, V, FK> implements KeyValueMapper<K, V, JoinKey> {
    private final JoinKeySchema<K, FK> keySchema;
    private final Function<V, FK> rightExtractor;

    public LeftKeyMapper(JoinKeySchema<K, FK> keySchema, Function<V, FK> rightExtractor) {
        this.keySchema = keySchema;
        this.rightExtractor = rightExtractor;
    }

    @Override
    public JoinKey apply(K key, V value) {
        final var fk = rightExtractor.apply(value);
        return keySchema.generateJoinKey(fk, key);
    }
}
