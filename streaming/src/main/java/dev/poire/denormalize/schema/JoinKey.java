package dev.poire.denormalize.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * Represents a key in the join index store and log. This key is specifically structured to be able
 * to be scanned by prefix using the right-side key. Each instance of this key represents an update on the join,
 * either on the left-side (both digests set), or on the right-side (right-side digest set).
 *
 * @param digestSize     The size of each digest (left and right)
 * @param rightKeyDigest The right-side key.
 * @param leftKeyDigest  The left-side key. If NULL, this key represents an update on the right-side of the join.
 */
@Slf4j
public record JoinKey(byte digestSize, byte[] rightKeyDigest, Optional<byte[]> leftKeyDigest) {
    public static final Serializer<JoinKey> serializer = (topic, key) -> {
        // [ digest length 1-255 ] [ right key digest ... ] [ nullable left key digest ... ]
        var concat = new byte[1 + key.digestSize * 2];

        // digest size
        concat[0] = key.digestSize;

        // right-side key (should not be NULL)
        System.arraycopy(key.rightKeyDigest, 0, concat, 1, key.digestSize);

        // left-side key (can be NULL)
        key.leftKeyDigest.ifPresent(left -> System.arraycopy(left, 0, concat, 1 + key.digestSize, key.digestSize));

        return concat;
    };

    public static final Deserializer<JoinKey> deserializer = (topic, concat) -> {
        final ByteBuffer buf = ByteBuffer.wrap(concat);

        var digestSize = buf.get();
        var right = new byte[digestSize];
        buf.get(right);

        var left = new byte[digestSize];
        buf.get(left);

        return new JoinKey(digestSize, right, Arrays.equals(left, /* NULL */ new byte[digestSize]) ? empty() : of(left));
    };

    public static final Serde<JoinKey> serde = Serdes.serdeFrom(serializer, deserializer);

    /**
     * Whether this key represents an update on the left-side of the join.
     *
     * @return true if this key represents an update on the left-side of the join.
     */
    public boolean isLeft() {
        return leftKeyDigest().isPresent();
    }

    /**
     * Whether this key represents an update on the right-side of the join.
     *
     * @return true if this key represents an update on the right-side of the join.
     */
    public boolean isRight() {
        return leftKeyDigest().isEmpty();
    }

    /**
     * Returns a new key as the right-side equivalent of this key. In other words,
     * it unsets the left-side key digest as a new key. This is used to look up the right-side value of a join in the index.
     *
     * @return The right-side equivalent of this key.
     */
    public JoinKey getRight() {
        return new JoinKey(digestSize, rightKeyDigest, empty());
    }

    /**
     * Serializes a trimmed version of this key, omitting the left-side digest. This is used to scan all keys
     * that contain the right-side digest of this key.
     *
     * @return The trimmed version of this key, including only the digest side and right-side digest.
     */
    public byte[] getPrefix() {
        final byte[] prefix = new byte[1 + digestSize];
        prefix[0] = digestSize;
        System.arraycopy(rightKeyDigest, 0, prefix, 1, digestSize);
        return prefix;
    }

    @Override
    public String toString() {
        // "[hex]-[hex]" pair
        return String.format("%s-%s", Hex.encodeHexString(rightKeyDigest), leftKeyDigest.map(Hex::encodeHexString).orElse("NULL"));
    }
}
