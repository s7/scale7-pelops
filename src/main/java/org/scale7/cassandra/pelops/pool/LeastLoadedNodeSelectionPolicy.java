package org.scale7.cassandra.pelops.pool;

import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Selects a node based on the number of connections a node currently has in use.  The node that has the least active
 * connections will be chosen.
 */
public class LeastLoadedNodeSelectionPolicy implements CommonsBackedPool.INodeSelectionPolicy {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(LeastLoadedNodeSelectionPolicy.class);

    @Override
    public CommonsBackedPool.PooledNode select(CommonsBackedPool pool, Map<String, CommonsBackedPool.PooledNode> nodes) {
        if (nodes.isEmpty())
            throw new IllegalStateException("There are no nodes to choose from");

        if (nodes.size() == 1)
            return nodes.values().iterator().next();

        // create a candidate list (otherwise the numActive could change while sorting)
        logger.debug("Determining which node is the least loaded");
        List<Candidate> candidates = new ArrayList<Candidate>(nodes.size());
        for (CommonsBackedPool.PooledNode pooledNode : nodes.values()) {
            int active = pooledNode.getNumActive();
            logger.debug("Node '{}' has {} active connections", pooledNode.getAddress(), active);
            candidates.add(new Candidate(pooledNode.getAddress(), active));
        }

        Collections.sort(candidates);

        return nodes.get(candidates.get(0).address);
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
