package dev.poire.denormalize.transform;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeyProvider;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class RightKeyMapper<FK, V> implements KeyValueMapper<FK, V, JoinKey> {

    private final JoinKeyProvider keyProvider;

    private final Serde<FK> foreignKeySerializer;

    public RightKeyMapper(JoinKeyProvider keyProvider, Serde<FK> foreignKeySerializer) {
        this.keyProvider = keyProvider;
        this.foreignKeySerializer = foreignKeySerializer;
    }

    @Override
    public JoinKey apply(FK fk, V value) {
        final var fkSer = foreignKeySerializer.serializer().serialize(null, fk);
        return keyProvider.generateRightJoinKey(fkSer);
    }
}
