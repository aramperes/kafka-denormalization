package dev.poire.streaming.denorm;

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

@Slf4j
public record JoinKey(byte digestSize, byte[] foreignKeyDigest, Optional<byte[]> primaryKeyDigest) {
    public static final Serializer<JoinKey> serializer = (topic, key) -> {
        // [ digest length 1-255 ] [ foreign key digest ... ] [ primary key digest ... ]
        var concat = new byte[1 + key.digestSize * 2];

        // digest size
        concat[0] = key.digestSize;

        // foreign key (right side of the join)
        System.arraycopy(key.foreignKeyDigest, 0, concat, 1, key.digestSize);

        // primary key (left side of the join)
        key.primaryKeyDigest.ifPresent(pk -> System.arraycopy(pk, 0, concat, 1 + key.digestSize, key.digestSize));

        return concat;
    };

    public static final Deserializer<JoinKey> deserializer = (topic, concat) -> {
        final ByteBuffer buf = ByteBuffer.wrap(concat);

        var digestSize = buf.get();
        var fk = new byte[digestSize];
        buf.get(fk);

        var pk = new byte[digestSize];
        buf.get(pk);

        return new JoinKey(digestSize, fk, Arrays.equals(pk, /* NULL */ new byte[digestSize]) ? empty() : of(pk));
    };

    public static final Serde<JoinKey> serde = Serdes.serdeFrom(serializer, deserializer);

    public boolean isLeft() {
        return primaryKeyDigest().isPresent();
    }

    public boolean isRight() {
        return primaryKeyDigest().isEmpty();
    }

    public JoinKey getRight() {
        return new JoinKey(digestSize, foreignKeyDigest, empty());
    }

    public byte[] getPrefix() {
        final byte[] prefix = new byte[1 + digestSize];
        prefix[0] = digestSize;
        System.arraycopy(foreignKeyDigest, 0, prefix, 1, digestSize);
        return prefix;
    }

    @Override
    public String toString() {
        // "[hex]-[hex]" pair
        return String.format("%s-%s", Hex.encodeHexString(foreignKeyDigest), primaryKeyDigest.map(Hex::encodeHexString).orElse("NULL"));
    }
}
