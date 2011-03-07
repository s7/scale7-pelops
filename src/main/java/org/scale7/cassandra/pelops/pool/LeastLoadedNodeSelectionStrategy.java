/*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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

            candidates.add(new Candidate(pooledNode.getAddress(), pooledNode));
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
        public Candidate(String address, PooledNode node) {
            this.address = address;
            this.numActive = node.getNumActive();
            this.numBorrowed = node.getConnectionsBorrowedTotal();
            this.numCorrupted = node.getConnectionsCorrupted();

            if (logger.isDebugEnabled())
                logger.debug("Node '{}' has {} active connections, {} borrowed connections and {} corrupted connections", new Object[] {address, numActive, numBorrowed, numCorrupted});
        }

        String address;
        int numActive;
        int numBorrowed;
        int numCorrupted;

        @Override
        public int compareTo(Candidate candidate) {
            int value = numActive - candidate.numActive;

            if (value == 0)
                value = numBorrowed - candidate.numBorrowed;

            if (value == 0)
                value = numCorrupted - candidate.numCorrupted;

            return value;
        }
    }
}
