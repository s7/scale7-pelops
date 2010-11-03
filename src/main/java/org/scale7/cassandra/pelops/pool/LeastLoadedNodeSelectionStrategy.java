package org.scale7.cassandra.pelops.pool;

import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.*;

/**
 * Selects a node based on the number of connections a node currently has in use.  The node that has the least active
 * connections will be chosen.
 */
public class LeastLoadedNodeSelectionStrategy implements CommonsBackedPool.INodeSelectionStrategy {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(LeastLoadedNodeSelectionStrategy.class);

    @Override
    public CommonsBackedPool.PooledNode select(CommonsBackedPool pool, Set<String> nodeAddresses, String notNodeHint) {
        // create a candidate list (otherwise the numActive could change while sorting)
        logger.debug("Determining which node is the least loaded");
        List<Candidate> candidates = new ArrayList<Candidate>(nodeAddresses.size());
        for (String nodeAddress : nodeAddresses) {
            CommonsBackedPool.PooledNode pooledNode = pool.getPooledNode(nodeAddress);
            if (pooledNode == null || pooledNode.isSuspended()) {
                logger.debug("Excluding node '{}' because it's either been removed from the pool or has been suspended", nodeAddress);
                continue;
            }
            int active = pooledNode.getNumActive();
            logger.debug("Node '{}' has {} active connections", pooledNode.getAddress(), active);
            candidates.add(new Candidate(pooledNode.getAddress(), active));
        }

        // make sure there's at least one node to choose from after filtering out suspended nodes etc
        if (nodeAddresses.isEmpty())
            return null;
        if (nodeAddresses.size() == 1)
            return pool.getPooledNode(nodeAddresses.iterator().next());

        // sort to find the node with the least active connections
        Collections.sort(candidates);

        // pick a node (trying to honor the notNodeHint)
        CommonsBackedPool.PooledNode node = null;
        for (Candidate candidate : candidates) {
            node = pool.getPooledNode(candidate.address);
            if (!candidate.address.equals(notNodeHint)) {
                break;
            } else {
                logger.debug("Attempting to honor the notNodeHint '{}', skipping node '{}'", notNodeHint, candidate.address);
                continue;
            }
        }

        logger.debug("Chose node '{}'...", node.getAddress());
        return node;
    }

    public class Candidate implements Comparable<Candidate> {
        public Candidate(String address, int numActive) {
            this.address = address;
            this.numActive = numActive;
        }

        String address;
        int numActive;

        @Override
        public int compareTo(Candidate candidate) {
            return numActive - candidate.numActive;
        }
    }
}
