package dev.poire.denormalize.transform;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeySchema;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class RightKeyMapper<R, V> implements KeyValueMapper<R, V, JoinKey> {

    private final JoinKeySchema<?, R> keySchema;

    public RightKeyMapper(JoinKeySchema<?, R> keySchema) {
        this.keySchema = keySchema;
    }

    @Override
    public JoinKey apply(R r, V value) {
        return keySchema.generateRightJoinKey(r);
    }
}
