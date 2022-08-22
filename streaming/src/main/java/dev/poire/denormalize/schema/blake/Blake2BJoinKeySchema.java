package dev.poire.denormalize.schema.blake;

import dev.poire.denormalize.schema.JoinKey;
import dev.poire.denormalize.schema.JoinKeySchema;
import org.apache.kafka.common.serialization.Serde;

import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * Generate composite join keys by hashing each side with "Blake2b" algorithm.
 */
public class Blake2BJoinKeySchema<L, R> implements JoinKeySchema<L, R> {
    private final byte digestSize;
    private final Serde<L> leftSerde;
    private final Serde<R> rightSerde;

    /**
     * Initializes the Blaze2b key provider, with the given digest size for each side.
     *
     * @param digestSize The size of each digest. Must be between 1 and 64, inclusive.
     * @param leftSerde  The left-side key serializer, that will then be passed through the hasher.
     * @param rightSerde The right-side key serializer, that will then be passed through the hasher.
     */
    public Blake2BJoinKeySchema(byte digestSize, Serde<L> leftSerde, Serde<R> rightSerde) {
        this.digestSize = digestSize;
        this.leftSerde = leftSerde;
        this.rightSerde = rightSerde;
    }

    @Override
    public JoinKey generateRightJoinKey(R right) {
        final byte[] rightDigest = new byte[digestSize];
        final byte[] rightSer = rightSerde.serializer().serialize(null, right);
        hash(rightSer, rightDigest);
        return new JoinKey(digestSize, rightDigest, empty());
    }

    @Override
    public JoinKey generateJoinKey(R right, L left) {
        final byte[] rightDigest = new byte[digestSize];
        final byte[] leftDigest = new byte[digestSize];

        final byte[] rightSer = rightSerde.serializer().serialize(null, right);
        final byte[] leftSer = leftSerde.serializer().serialize(null, left);

        hash(rightSer, rightDigest);
        hash(leftSer, leftDigest);
        return new JoinKey(digestSize, rightDigest, of(leftDigest));
    }

    private void hash(byte[] part, byte[] output) {
        final Blake2b.Digest digest = Blake2b.Digest.newInstance(digestSize);
        digest.update(part);
        digest.digest(output, 0, digestSize);
    }

    @Override
    public Serde<L> leftSerde() {
        return leftSerde;
    }

    @Override
    public Serde<R> rightSerde() {
        return rightSerde;
    }
}
