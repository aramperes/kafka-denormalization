package dev.poire.denormalize.transform;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeyProvider;
import dev.poire.denormalize.schema.JoinKeyProviders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.function.BiFunction;

public class LeftKeyMapper<K, V, FK> implements KeyValueMapper<K, V, JoinKey> {

    private final JoinKeyProvider keyProvider;

    private final Serde<K> leftSerializer;
    private final Serde<FK> rightSerializer;

    private final BiFunction<K, V, FK> rightExtractor;

    public LeftKeyMapper() {
        // Defaults
        this.keyProvider = JoinKeyProviders.Blake2b((byte) 8);
        this.leftSerializer = null;
        this.rightSerializer = null;
        this.rightExtractor = null;
    }

    public LeftKeyMapper(JoinKeyProvider keyProvider, Serde<K> leftSerializer, Serde<FK> rightSerializer, BiFunction<K, V, FK> rightExtractor) {
        this.keyProvider = keyProvider;
        this.leftSerializer = leftSerializer;
        this.rightSerializer = rightSerializer;
        this.rightExtractor = rightExtractor;
    }

    @Override
    public JoinKey apply(K key, V value) {
        final var fk = rightExtractor.apply(key, value);
        final var keySer = leftSerializer.serializer().serialize(null, key);
        final var fkSer = rightSerializer.serializer().serialize(null, fk);
        return keyProvider.generateJoinKey(fkSer, keySer);
    }
}
