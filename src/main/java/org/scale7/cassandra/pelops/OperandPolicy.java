package org.scale7.cassandra.pelops;

/**
 * General policy configuration.
 */
public class OperandPolicy {
    int maxOpRetries = 3;
    boolean deleteIfNull = false;

    public OperandPolicy() {
    }

    public OperandPolicy(int maxOpRetries, boolean deleteIfNull) {
        this.maxOpRetries = maxOpRetries;
        this.deleteIfNull = deleteIfNull;
    }

    public int getMaxOpRetries() {
        return maxOpRetries;
    }

    /**
     * Max number of times to retry an operation before giving up.
     * Default to 2.
     * @param maxOpRetries the value
     */
    public void setMaxOpRetries(int maxOpRetries) {
        this.maxOpRetries = maxOpRetries;
    }

    /**
     * Dictates if pelops should issue deletes when it detects null values being written in a mutation batch.
     * @return true if deletes should be issued by default
     */
    public boolean isDeleteIfNull() {
        return deleteIfNull;
    }

    /**
     * Dictates if pelops should issue deletes when it detects null values being written in a mutation batch.
     * @param deleteIfNull true if deletes should be issued by default
     */
    public void setDeleteIfNull(boolean deleteIfNull) {
        this.deleteIfNull = deleteIfNull;
    }
}
