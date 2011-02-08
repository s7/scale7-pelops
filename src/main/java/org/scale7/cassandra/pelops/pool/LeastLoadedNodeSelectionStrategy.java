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
    public PooledNode select(CommonsBackedPool pool, Set<String> nodeAddresses, Set<String> avoidNodesHint) {
        // create a candidate list (otherwise the numActive could change while sorting)
        if (logger.isDebugEnabled())
            logger.debug("Determining which node is the least loaded");
        List<Candidate> candidates = new ArrayList<Candidate>(nodeAddresses.size());
        for (String nodeAddress : nodeAddresses) {
            PooledNode pooledNode = pool.getPooledNode(nodeAddress);
            if (pooledNode == null || pooledNode.isSuspended()) {
                if (logger.isDebugEnabled())
                    logger.debug("Excluding node '{}' because it's either been removed from the pool or has been suspended", nodeAddress);
                continue;
            }
            int active = pooledNode.getNumActive();
            if (logger.isDebugEnabled())
                logger.debug("Node '{}' has {} active connections", pooledNode.getAddress(), active);
            candidates.add(new Candidate(pooledNode.getAddress(), active));
        }

        // make sure there's at least one node to choose from after filtering out suspended nodes etc
        if (candidates.isEmpty())
            return null;
        if (candidates.size() == 1)
            return pool.getPooledNode(candidates.iterator().next().address);

        // sort to find the node with the least active connections
        Collections.sort(candidates);

        // pick a node (trying to honor the notNodeHint)
        PooledNode node = null;
        for (Candidate candidate : candidates) {
            node = pool.getPooledNode(candidate.address);
            if (avoidNodesHint == null || !avoidNodesHint.contains(candidate.address)) {
                break;
            } else {
                if (logger.isDebugEnabled())
                    logger.debug("Attempting to honor the avoidNodesHint '{}', skipping node '{}'", avoidNodesHint, candidate.address);
                continue;
            }
        }

        if (logger.isDebugEnabled())
            logger.debug("Chose node '{}'...", node != null ? node.getAddress() : "null");

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
