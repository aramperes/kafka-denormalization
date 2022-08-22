package dev.poire.denormalize.transform;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeyProvider;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.function.BiFunction;

public class LeftKeyMapper<K, V, FK> implements KeyValueMapper<K, V, JoinKey> {
    private final JoinKeyProvider<K, FK> keyProvider;
    private final BiFunction<K, V, FK> rightExtractor;

    public LeftKeyMapper(JoinKeyProvider<K, FK> keyProvider, BiFunction<K, V, FK> rightExtractor) {
        this.keyProvider = keyProvider;
        this.rightExtractor = rightExtractor;
    }

    @Override
    public JoinKey apply(K key, V value) {
        final var fk = rightExtractor.apply(key, value);
        return keyProvider.generateJoinKey(fk, key);
    }
}
