package dev.poire.denormalize.schema;

import dev.poire.denormalize.schema.blake.Blake2BJoinKeySchema;
import org.apache.kafka.common.serialization.Serde;

public final class JoinKeySchemas {

    /**
     * Generate composite join keys by hashing each side with "Blake2b" algorithm of the given size.
     *
     * @param digestSize The size of each digest. Must be between 1 and 64, inclusive.
     * @return The join key provider.
     */
    public static <L, R> JoinKeySchema<L, R> Blake2b(int digestSize, Serde<L> leftSerde, Serde<R> rightSerde) {
        return new Blake2BJoinKeySchema<>((byte) digestSize, leftSerde, rightSerde);
    }
}
