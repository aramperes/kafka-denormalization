package dev.poire.denormalize.schema;

import dev.poire.denormalize.transform.LeftKeyMapper;
import dev.poire.denormalize.transform.RightKeyMapper;
import org.apache.kafka.common.serialization.Serde;

import java.util.function.Function;

/**
 * Use this interface to digest and combine two keys into one.
 */
public interface JoinKeySchema<L, R> {

    /**
     * Hashes and wraps a right-side update key. The algorithm implemented here must produce a fixed-size digest.
     *
     * @param right The right-side update key.
     * @return The generated JoinKey.
     */
    JoinKey generateRightJoinKey(R right);

    /**
     * Hashes and wraps a left-side update key. The algorithm implemented here must produce a fixed-size digest.
     *
     * @param right The right-side update key.
     * @param left  The left-side update key.
     * @return The generated JoinKey.
     */
    JoinKey generateJoinKey(R right, L left);

    Serde<L> leftSerde();

    Serde<R> rightSerde();

    default <V> LeftKeyMapper<L, V, R> joinOn(Function<V, R> rightExtractor) {
        return new LeftKeyMapper<>(this, rightExtractor);
    }

    default <V> RightKeyMapper<R, V> right() {
        return new RightKeyMapper<>(this);
    }
}
