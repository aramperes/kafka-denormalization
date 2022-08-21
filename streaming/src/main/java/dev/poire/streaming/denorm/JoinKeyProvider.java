package dev.poire.streaming.denorm;

public interface JoinKeyProvider {

    JoinKey generateRightJoinKey(byte[] right);

    JoinKey generateJoinKey(byte[] right, byte[] left);
}
