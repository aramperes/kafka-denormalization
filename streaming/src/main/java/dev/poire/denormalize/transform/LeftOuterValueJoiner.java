package dev.poire.denormalize.transform;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Optional;

public interface LeftOuterValueJoiner<V, FV, VR> extends ValueJoiner<V, Optional<FV>, VR> {
}
