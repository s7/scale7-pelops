package org.scale7.cassandra.pelops.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Sets;

/**
 * Tests the {@link LeastLoadedNodeSelectionStrategy} class.
 */
public class LeastLoadedNodeSelectionStrategyUnitTest {
    /**
     * Test to verify that if all nodes are are suspended that null is returned.
     */
    @Test
    public void testAllNodesSuspended() {
        Set<String> nodeAddresses = new HashSet<String>(Arrays.asList("node1", "node2", "node3"));
        CommonsBackedPool pool = Mockito.mock(CommonsBackedPool.class);

        // setup each pooled node to report that it's suspended
        for (String nodeAddress : nodeAddresses) {
            mockPoolMethods(pool, nodeAddress, new PooledNode(pool, nodeAddress) {
                @Override
                public boolean isSuspended() {
                    return true;
                }
            });
        }

        LeastLoadedNodeSelectionStrategy strategy = new LeastLoadedNodeSelectionStrategy();

        PooledNode node = strategy.select(pool, nodeAddresses, null);

        assertNull("No nodes should have been returned", node);
    }

    /**
     * Test to verify that if all but one of the nodes are suspended that that node is returned.
     */
    @Test
    public void testOnlyOneCandidateNode() {
        Set<String> nodeAddresses = new HashSet<String>(Arrays.asList("node1", "node2", "node3"));
        CommonsBackedPool pool = Mockito.mock(CommonsBackedPool.class);

        // setup each pooled node to report that it's suspended
        for (String nodeAddress : nodeAddresses) {
            mockPoolMethods(pool, nodeAddress, new PooledNode(pool, nodeAddress) {
                @Override
                public boolean isSuspended() {
                    return true;
                }
            });
        }

        // setup one node to be good
        final String goodNodeAddress = "node4";
        nodeAddresses.add(goodNodeAddress);
        mockPoolMethods(pool, goodNodeAddress, new PooledNode(pool, goodNodeAddress) {
            @Override
            public boolean isSuspended() {
                return false;
            }

            @Override
            public int getNumActive() {
                return 1;
            }

            @Override
            public int getConnectionsCorrupted() {
                return 0;
            }
        });

        LeastLoadedNodeSelectionStrategy strategy = new LeastLoadedNodeSelectionStrategy();

        PooledNode node = strategy.select(pool, nodeAddresses, null);

        assertNotNull("No nodes were returned from the pool", node);
        assertEquals("Wrong node returned from the pool", goodNodeAddress, node.getAddress());
    }

    /**
     * Test to verify that the node with the least active connections is selected.
     */
    @Test
    public void testLeastLoadedNodeSelected() {
        String leastLoadedNodeAddress = "node1";
        List<String> nodeAddresses = Arrays.asList(leastLoadedNodeAddress, "node2", "node3", "node4", "node5");
        CommonsBackedPool pool = Mockito.mock(CommonsBackedPool.class);

        // setup each pooled node to report it's number of active connections
        for (int i = 0; i < nodeAddresses.size(); i++) {
            final int numActive = i;
            mockPoolMethods(pool, nodeAddresses.get(i), new PooledNode(pool, nodeAddresses.get(i)) {
                @Override
                public boolean isSuspended() {
                    return false;
                }

                @Override
                public int getNumActive() {
                    return numActive;
                }

                @Override
                public int getConnectionsCorrupted() {
                    return 0;
                }
            });
        }

        LeastLoadedNodeSelectionStrategy strategy = new LeastLoadedNodeSelectionStrategy();

        PooledNode node = strategy.select(pool, new HashSet<String>(nodeAddresses), null);

        assertNotNull("No nodes were returned from the pool", node);
        assertEquals("Wrong node returned from the pool", leastLoadedNodeAddress, node.getAddress());
    }

    /**
     * Test to verify that when all nodes have an equal number of active and borrowed nodes then the node with the least corrupted
     * connections is selected.
     */
    @Test
    public void testNodesEqualThenLeastCorruptedSelected() {
        String leastLoadedNodeAddress = "node5";
        final List<String> nodeAddresses = Arrays.asList("node1", "node2", "node3", "node4", leastLoadedNodeAddress);
        CommonsBackedPool pool = Mockito.mock(CommonsBackedPool.class);

        // setup each pooled node to report it's number of active connections
        for (int i = 0; i < nodeAddresses.size(); i++) {
            final int numCorrupted = i;
            mockPoolMethods(pool, nodeAddresses.get(i), new PooledNode(pool, nodeAddresses.get(i)) {
                @Override
                public boolean isSuspended() {
                    return false;
                }

                @Override
                public int getNumActive() {
                    return 0;
                }

                @Override
                public int getConnectionsBorrowedTotal() {
                    return 0;
                }

                @Override
                public int getConnectionsCorrupted() {
                    return nodeAddresses.size() - numCorrupted;
                }
            });
        }

        LeastLoadedNodeSelectionStrategy strategy = new LeastLoadedNodeSelectionStrategy();

        PooledNode node = strategy.select(pool, new HashSet<String>(nodeAddresses), null);

        assertNotNull("No nodes were returned from the pool", node);
        assertEquals("Wrong node returned from the pool", leastLoadedNodeAddress, node.getAddress());
    }

    /**
     * Test to verify that when all nodes have an equal number of active and borrowed nodes then the node with the least corrupted
     * connections is selected.
     */
    @Test
    public void testNodesEqualThenLeastBorrowedSelected() {
        String leastLoadedNodeAddress = "node5";
        final List<String> nodeAddresses = Arrays.asList("node1", "node2", "node3", "node4", leastLoadedNodeAddress);
        CommonsBackedPool pool = Mockito.mock(CommonsBackedPool.class);

        // setup each pooled node to report it's number of active connections
        for (int i = 0; i < nodeAddresses.size(); i++) {
            final int numCorrupted = i;
            mockPoolMethods(pool, nodeAddresses.get(i), new PooledNode(pool, nodeAddresses.get(i)) {
                @Override
                public boolean isSuspended() {
                    return false;
                }

                @Override
                public int getNumActive() {
                    return 0;
                }

                @Override
                public int getConnectionsBorrowedTotal() {
                    return nodeAddresses.size() - numCorrupted;
                }

                @Override
                public int getConnectionsCorrupted() {
                    return 0;
                }
            });
        }

        LeastLoadedNodeSelectionStrategy strategy = new LeastLoadedNodeSelectionStrategy();

        PooledNode node = strategy.select(pool, new HashSet<String>(nodeAddresses), null);

        assertNotNull("No nodes were returned from the pool", node);
        assertEquals("Wrong node returned from the pool", leastLoadedNodeAddress, node.getAddress());
    }

    /**
     * Test to verify that the node with the least active connections is skipped because of the notNodeHint.
     */
    @Test
    public void testAvoidNodesHint() {
        String avoidNode = "node1";
        String selectedNodeAddress = "node2";
        List<String> nodeAddresses = Arrays.asList(avoidNode, selectedNodeAddress, "node3", "node4", "node5");
        CommonsBackedPool pool = Mockito.mock(CommonsBackedPool.class);

        // setup each pooled node to report it's number of active connections
        for (int i = 0; i < nodeAddresses.size(); i++) {
            final int numActive = i;
            mockPoolMethods(pool, nodeAddresses.get(i), new PooledNode(pool, nodeAddresses.get(i)) {
                @Override
                public boolean isSuspended() {
                    return false;
                }

                @Override
                public int getNumActive() {
                    return numActive;
                }

                @Override
                public int getConnectionsCorrupted() {
                    return 0;
                }
            });
        }

        LeastLoadedNodeSelectionStrategy strategy = new LeastLoadedNodeSelectionStrategy();

        PooledNode node = strategy.select(pool, new HashSet<String>(nodeAddresses), Sets.newHashSet(avoidNode));

        assertNotNull("No nodes were returned from the pool", node);
        assertEquals("Wrong node returned from the pool", selectedNodeAddress, node.getAddress());
    }

    /**
     * Test to verify that the node with the least active connections is skipped because of the notNodeHint.
     */
    @Test
    public void testAvoidNodesHintMultiple() {
        String avoidNode1 = "node1";
        String avoidNode2 = "node2";
        String selectedNodeAddress = "node3";
        List<String> nodeAddresses = Arrays.asList(avoidNode1, avoidNode2, selectedNodeAddress, "node4", "node5");
        CommonsBackedPool pool = Mockito.mock(CommonsBackedPool.class);

        // setup each pooled node to report it's number of active connections
        for (int i = 0; i < nodeAddresses.size(); i++) {
            final int numActive = i;
            mockPoolMethods(pool, nodeAddresses.get(i), new PooledNode(pool, nodeAddresses.get(i)) {
                @Override
                public boolean isSuspended() {
                    return false;
                }

                @Override
                public int getNumActive() {
                    return numActive;
                }

                @Override
                public int getConnectionsCorrupted() {
                    return 0;
                }
            });
        }

        LeastLoadedNodeSelectionStrategy strategy = new LeastLoadedNodeSelectionStrategy();

        PooledNode node = strategy.select(pool, new HashSet<String>(nodeAddresses), Sets.newHashSet(avoidNode1, avoidNode2));

        assertNotNull("No nodes were returned from the pool", node);
        assertEquals("Wrong node returned from the pool", selectedNodeAddress, node.getAddress());
    }

    private void mockPoolMethods(CommonsBackedPool pool, final String nodeAddress, final PooledNode pooledNode) {
        Mockito.when(pool.getPooledNode(nodeAddress)).thenAnswer(new Answer<PooledNode>() {
            @Override
            public PooledNode answer(InvocationOnMock invocation) throws Throwable {
                return pooledNode;
            }
        });
    }
}
