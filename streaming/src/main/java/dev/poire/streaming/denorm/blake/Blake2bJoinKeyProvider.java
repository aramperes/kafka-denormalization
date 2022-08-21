package dev.poire.streaming.denorm.blake;

import dev.poire.streaming.denorm.JoinKey;
import dev.poire.streaming.denorm.JoinKeyProvider;

import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * Generate composite join keys by hashing each size with "Blake2b" algorithm.
 */
public class Blake2bJoinKeyProvider implements JoinKeyProvider {
    private final byte digestSize;

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
