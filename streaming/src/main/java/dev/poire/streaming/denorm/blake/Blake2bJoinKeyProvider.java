package dev.poire.streaming.denorm.blake;

import dev.poire.streaming.denorm.JoinKeyProvider;

/**
 * Generate composite join keys by hashing each size with "Blake2b" algorithm.
 */
public class Blake2bJoinKeyProvider implements JoinKeyProvider {
    private final int digestSize;

    public Blake2bJoinKeyProvider(int digestSize) {
        this.digestSize = digestSize;
    }

    @Override
    public int getKeyByteSize() {
        return digestSize * 2;
    }

    @Override
    public byte[] generateRightJoinKey(byte[] right) {
        final byte[] key = new byte[digestSize * 2];
        hash(right, key, 0);
        return key;
    }

    @Override
    public byte[] generateJoinKey(byte[] right, byte[] left) {
        final byte[] key = new byte[digestSize * 2];
        hash(right, key, 0);
        hash(left, key, 8);
        return key;
    }

    private void hash(byte[] part, byte[] output, int offset) {
        final Blake2b.Digest digest = Blake2b.Digest.newInstance(digestSize);
        digest.update(part);
        digest.digest(output, offset, digestSize);
    }
}
