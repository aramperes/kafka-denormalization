package dev.poire.streaming.pipeline;

import dev.poire.streaming.denorm.blake.Blake2bJoinKeyProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
@Slf4j
public class Stream {

    @Value("${topics.stories}")
    private String topicStories;

    @Value("${topics.comments}")
    private String topicComments;

    @Value("${topics.joined}")
    private String topicJoined;

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        var commentId = "1234";
        var storyId = "5678";
        var prov = new Blake2bJoinKeyProvider(8);
        log.info("JOIN 1-to-many ({},{})   = {}", storyId, commentId, Arrays.toString(prov.generateJoinKey(storyId.getBytes(), commentId.getBytes())));
        log.info("JOIN 1-to-many ({},NULL) = {}", storyId, Arrays.toString(prov.generateRightJoinKey(storyId.getBytes())));
    }
}
