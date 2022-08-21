package dev.poire.streaming.pipeline;

import dev.poire.streaming.denorm.JoinKey;
import dev.poire.streaming.denorm.JoinKeyProvider;
import dev.poire.streaming.denorm.blake.Blake2bJoinKeyProvider;
import dev.poire.streaming.dto.Comment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;

@Component
@Slf4j
public class Stream {

    @Value("${topics.stories}")
    private String topicStories;

    @Value("${topics.comments}")
    private String topicComments;

    @Value("${topics.index}")
    private String topicIndex;

    @Value("${topics.joined}")
    private String topicJoined;

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        final var keyProvider = new Blake2bJoinKeyProvider((byte) 8);

        // Left side of the join
        // Every time a LEFT is received, it will forward it to the INDEX topic, but re-keyed to include the foreign key,
        // and also, it will manually partition the output based on the foreign key only.
        builder.stream(topicComments, Consumed.with(Serdes.String(), Comment.serde))
                .selectKey(new LeftKeyMapper<>(keyProvider, Serdes.String(), Serdes.String(), (k, comment) -> comment.story().toString()))
                .to(topicIndex, Produced.with(JoinKey.serde, Comment.serde).withStreamPartitioner(new ForeignKeyPartitioner<>()));
    }

    static class ForeignKeyPartitioner<V> implements StreamPartitioner<JoinKey, V> {
        @Override
        public Integer partition(String topic, JoinKey key, V value, int numPartitions) {
            return Utils.toPositive(Utils.murmur2(key.foreignKeyDigest())) % numPartitions;
        }
    }

    static class LeftKeyMapper<K, V, FK> implements KeyValueMapper<K, V, JoinKey> {

        private final JoinKeyProvider keyProvider;

        private final Serde<K> keySerializer;
        private final Serde<FK> foreignKeySerializer;

        private final BiFunction<K, V, FK> foreignKeyExtractor;

        public LeftKeyMapper(JoinKeyProvider keyProvider, Serde<K> keySerializer, Serde<FK> foreignKeySerializer, BiFunction<K, V, FK> foreignKeyExtractor) {
            this.keyProvider = keyProvider;
            this.keySerializer = keySerializer;
            this.foreignKeySerializer = foreignKeySerializer;
            this.foreignKeyExtractor = foreignKeyExtractor;
        }

        @Override

        public JoinKey apply(K key, V value) {
            final var fk = foreignKeyExtractor.apply(key, value);
            final var keySer = keySerializer.serializer().serialize(null, key);
            final var fkSer = foreignKeySerializer.serializer().serialize(null, fk);
            return keyProvider.generateJoinKey(fkSer, keySer);
        }
    }

}
