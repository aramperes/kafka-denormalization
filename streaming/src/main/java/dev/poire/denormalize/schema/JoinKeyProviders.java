package dev.poire.denormalize.schema;

import dev.poire.denormalize.schema.blake.Blake2bJoinKeyProvider;

public final class JoinKeyProviders {

    /**
     * Generate composite join keys by hashing each side with "Blake2b" algorithm of the given size.
     *
     * @param digestSize The size of each digest. Must be between 1 and 64, inclusive.
     * @return The join key provider.
     */
    public static JoinKeyProvider Blake2b(byte digestSize) {
        return new Blake2bJoinKeyProvider(digestSize);
    }
}
