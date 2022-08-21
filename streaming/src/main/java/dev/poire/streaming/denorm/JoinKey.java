package dev.poire.streaming.denorm;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;

@Slf4j
public record JoinKey(byte digestLength, byte[] foreignKeyDigest, byte[] primaryKeyDigest) {
    public static final Serializer<JoinKey> serializer = (topic, key) -> {
        // [ digest length ] [ foreign key digest ... ] [ primary key digest ... ]
        var concat = new byte[1 + key.digestLength * 2];

        concat[0] = key.digestLength;
        System.arraycopy(key.foreignKeyDigest, 0, concat, 1, key.digestLength);
        System.arraycopy(key.primaryKeyDigest, 0, concat, 1 + key.digestLength, key.digestLength);

        log.info("Serialized join-key ({},{}) to {}", Arrays.toString(key.foreignKeyDigest), Arrays.toString(key.primaryKeyDigest), Arrays.toString(concat));
        return concat;
    };

    public static final Deserializer<JoinKey> deserializer = (topic, concat) -> {
        var digestLength = concat[0];
        var foreignKeyDigest = new byte[digestLength];
        var primaryKeyDigest = new byte[digestLength];
        System.arraycopy(concat, 1, foreignKeyDigest, 0, digestLength);
        System.arraycopy(concat, 1 + digestLength, primaryKeyDigest, 0, digestLength);
        return new JoinKey(digestLength, foreignKeyDigest, primaryKeyDigest);
    };

    public static final Serde<JoinKey> serde = Serdes.serdeFrom(serializer, deserializer);
}
