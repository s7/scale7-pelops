package org.scale7.cassandra.pelops;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests the {@link org.scale7.cassandra.pelops.Cluster} class.
 */
public class ClusterUnitTest {
    /**
     * Tests that if multiple nodes are provided in a single string that they it is processed correctly.
     */
    @Test
    public void testConstructorNodesSplit() {
        String[] nodes = new String[] {"node1", "node2", "node3"};

        Cluster cluster = new Cluster(nodes[0] + ", " + nodes[1] + ", " + nodes[2], 5555, false);
        Cluster.Node[] resultingNodes = cluster.getNodes();
        assertEquals("Incorrect wrong number of contact nodes", 3, resultingNodes.length);
        for (int i = 0; i < nodes.length; i++) {
            assertEquals("Node did not match", nodes[i], resultingNodes[i].getAddress());
        }
    }

    /**
     * Tests that if a single node is provided in a single string that it is processed correctly.
     */
    @Test
    public void testConstructorNodesSplitSingleNode() {
        String node = "node1";
        Cluster cluster = new Cluster(node, 5555, false);
        Cluster.Node[] resultingNodes = cluster.getNodes();
        assertEquals("Incorrect wrong number of contact nodes", 1, resultingNodes.length);
        assertEquals("Node did not match", node, resultingNodes[0].getAddress());
    }
}
