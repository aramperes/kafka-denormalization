package dev.poire.denormalize.transform;

public class KeyMapper {
    public static <K, V, FK> LeftKeyMapper<K, V, FK> left() {
        return new LeftKeyMapper<>();
    }
}
