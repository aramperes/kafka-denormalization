package dev.poire.denormalize.transform;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeyProvider;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class RightKeyMapper<R, V> implements KeyValueMapper<R, V, JoinKey> {

    private final JoinKeyProvider<?, R> keyProvider;

    public RightKeyMapper(JoinKeyProvider<?, R> keyProvider) {
        this.keyProvider = keyProvider;
    }

    @Override
    public JoinKey apply(R r, V value) {
        return keyProvider.generateRightJoinKey(r);
    }
}
