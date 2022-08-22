package dev.poire.denormalize.schema;

/**
 * Use this interface to digest and combine two keys into one.
 */
public interface JoinKeyProvider {

    /**
     * Hashes and wraps a right-side update key. The algorithm implemented here must produce a fixed-size digest.
     *
     * @param right The right-side update key.
     * @return The generated JoinKey.
     */
    JoinKey generateRightJoinKey(byte[] right);

    /**
     * Hashes and wraps a left-side update key. The algorithm implemented here must produce a fixed-size digest.
     *
     * @param right The right-side update key.
     * @param left  The left-side update key.
     * @return The generated JoinKey.
     */
    JoinKey generateJoinKey(byte[] right, byte[] left);
}
