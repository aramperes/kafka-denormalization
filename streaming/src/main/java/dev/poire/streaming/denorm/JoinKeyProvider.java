package dev.poire.streaming.denorm;

public interface JoinKeyProvider {
    int getKeyByteSize();

    byte[] generateRightJoinKey(byte[] right);

    byte[] generateJoinKey(byte[] right, byte[] left);
}
