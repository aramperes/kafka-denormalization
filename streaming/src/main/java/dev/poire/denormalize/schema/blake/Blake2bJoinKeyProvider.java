package dev.poire.denormalize.schema.blake;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeyProvider;

import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * Generate composite join keys by hashing each side with "Blake2b" algorithm.
 */
public class Blake2bJoinKeyProvider implements JoinKeyProvider {
    private final byte digestSize;

    /**
     * Initializes the Blaze2b key provider, with the given digest size for each side.
     *
     * @param digestSize The size of each digest. Must be between 1 and 64, inclusive.
     */
    public Blake2bJoinKeyProvider(byte digestSize) {
        this.digestSize = digestSize;
    }

    @Override
    public JoinKey generateRightJoinKey(byte[] right) {
        final byte[] rightDigest = new byte[digestSize];
        hash(right, rightDigest);
        return new JoinKey(digestSize, rightDigest, empty());
    }

    @Override
    public JoinKey generateJoinKey(byte[] right, byte[] left) {
        final byte[] rightDigest = new byte[digestSize];
        final byte[] leftDigest = new byte[digestSize];
        hash(right, rightDigest);
        hash(left, leftDigest);
        return new JoinKey(digestSize, rightDigest, of(leftDigest));
    }

    private void hash(byte[] part, byte[] output) {
        final Blake2b.Digest digest = Blake2b.Digest.newInstance(digestSize);
        digest.update(part);
        digest.digest(output, 0, digestSize);
    }
}
