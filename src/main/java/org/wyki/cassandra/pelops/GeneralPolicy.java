package org.wyki.cassandra.pelops;

/**
 * General policy configuration.
 */
public class GeneralPolicy {
    int maxOpRetries = 3;

    public GeneralPolicy() {
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
}
