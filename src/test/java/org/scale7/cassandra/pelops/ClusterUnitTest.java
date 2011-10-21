package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests the {@link org.scale7.cassandra.pelops.Cluster} class.
 */
public class ClusterUnitTest {
	
	private static final String USERNAME = "tester";
	private static final String PASSWORD = "password";
	
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
    
    /**
     * Tests that a connection authenticator on a cluster is configured correctly
     */
    @Test
    public void testClusterNodeConnectionAuthenticator() {
        String node = "node1";
        Cluster cluster = new Cluster(node, 5555, false, new SimpleConnectionAuthenticator(USERNAME,PASSWORD));
        assertNotNull("Incorrect connection authentication config",cluster.getConnectionConfig().getConnectionAuthenticator());
        assertEquals("Incorrect connection authenticator class"
        		,cluster.getConnectionConfig().getConnectionAuthenticator().getClass(),SimpleConnectionAuthenticator.class);
        AuthenticationRequest request = cluster.getConnectionConfig().getConnectionAuthenticator().getAuthenticationRequest();
        assertNotNull("Invalid authentication request",request);
        assertNotNull("Invalid authentication request credentials",request.getCredentials());
        assertEquals("Invalid authentication username",request.getCredentials().get(SimpleConnectionAuthenticator.USERNAME_KEY),USERNAME);
        assertEquals("Invalid authentication username",request.getCredentials().get(SimpleConnectionAuthenticator.PASSWORD_KEY),PASSWORD);
    }
    
    /**
     * Tests that a cluster without a connection authenticator returns a null authenticator
     */
    @Test
    public void testClusterNodeWithoutConnectionAuthenticator() {
        String node = "node1";
        Cluster cluster = new Cluster(node, 5555, false);
        assertNull("Connection authentication should be null",cluster.getConnectionConfig().getConnectionAuthenticator());
    }
}
