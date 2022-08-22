package dev.poire.denormalize.transform;

import dev.poire.denormalize.schema.JoinKey;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * A custom partitioner for {@link JoinKey} that uses the right-side key to pick the partition.
 * <p>
 * This partitioner must be used when populating the index topic to ensure that all left-side AND right-side updates
 * for the common key (right) are on the same partition.
 *
 * @param <V> The value type in this topic. Unused for this partitioner.
 */
@SuppressWarnings("all")
public final class JoinKeyPartitioner<V> implements StreamPartitioner<JoinKey, V> {
    private static final JoinKeyPartitioner INSTANCE = new JoinKeyPartitioner();

    @Override
    public Integer partition(String topic, JoinKey key, V value, int numPartitions) {
        return Utils.toPositive(Utils.murmur2(key.rightKeyDigest())) % numPartitions;
    }

    /**
     * A custom partitioner for {@link JoinKey} that uses the right-side key to pick the partition.
     * <p>
     * This partitioner must be used when populating the index topic to ensure that all left-side AND right-side updates
     * for the common key (right) are on the same partition.
     *
     * @param <V> The value type in this topic. Unused for this partitioner.
     */
    public static <V> JoinKeyPartitioner<V> partitioner() {
        return INSTANCE;
    }
}
