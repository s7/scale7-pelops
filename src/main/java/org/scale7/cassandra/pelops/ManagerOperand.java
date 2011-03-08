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

package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.transport.TTransportException;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

public class ManagerOperand {
	private static final Logger logger = SystemProxy.getLoggerFromFactory(ManagerOperand.class);

	static final int SAFE_NODE_CHANGE_DELAY = 60000;
	static final int RETRY_NODE_DELAY = 1000;
	static final int CHANGE_NODE_DELAY = 500;
	static final int MAX_ATTEMPTS = 8;

	private Cluster cluster;
	private String keyspace;
	private Cluster.Node[] nodesSnapshot;
    private int chosenNodeIdx = 0;
	private long lastNodeWrite = 0;
	private int safeNodeChangeDelay;
    private IConnection connection;

	ManagerOperand(Cluster cluster, String keyspace, int safeNodeChangeDelay) {
    	this.cluster = cluster;
    	this.keyspace = keyspace;
    	this.safeNodeChangeDelay = safeNodeChangeDelay;
    	nodesSnapshot = cluster.getNodes();
	}

	ManagerOperand(Cluster cluster, String keyspace) {
		this(cluster, keyspace, SAFE_NODE_CHANGE_DELAY);
	}

    ManagerOperand(Cluster cluster) {
    	this(cluster, null, SAFE_NODE_CHANGE_DELAY);
    }

    ManagerOperand(Cluster cluster, int safeNodeChangeDelay) {
        this(cluster, null, safeNodeChangeDelay);
    }

    /**
     * Acquire connections to cluster nodes in a manner that reduces the likelihood of synchronization issues
     * in the event of failure. See http://wiki.apache.org/cassandra/FAQ#no_keyspaces for an explanation.
     * @return
     * @throws Exception
     */
	protected void openClient() throws Exception {
		int attempts = 0;
        Cluster.Node chosenNode = null;
		while (true) {
			try {
                chosenNode = nodesSnapshot[chosenNodeIdx];

                if (logger.isDebugEnabled())
                    logger.debug("Attempting operation against node '{}'...", chosenNode.getAddress());
                
                connection = new Connection(chosenNode, keyspace);
                connection.open();

	            lastNodeWrite = System.currentTimeMillis();
	            return;
			} catch (Exception e) {
				closeClient();
				attempts++;
				if (attempts < MAX_ATTEMPTS &&
					(e instanceof TimedOutException ||
	                 e instanceof TTransportException ||
	                 e instanceof UnavailableException)) {
					if ((System.currentTimeMillis() - lastNodeWrite) < safeNodeChangeDelay) {
						logger.warn("Retrying opening connection to same node after previous failure to avoid potential synchronization issues");
						Thread.sleep(RETRY_NODE_DELAY);
					} else {
                        logger.warn("Failed management operation against node '{}'", chosenNode != null ? chosenNode.getAddress() : "null");
						chosenNodeIdx++;
						if (chosenNodeIdx == nodesSnapshot.length) {
							chosenNodeIdx = 0;
							nodesSnapshot = cluster.getNodes();
						}
						Thread.sleep(CHANGE_NODE_DELAY);
					}
				}
				else {
					throw e;
				}
			}
		}
	}

	private void closeClient() {
        if (connection != null)
            connection.close();
	}

	protected interface IManagerOperation<ReturnType> {
		ReturnType execute(Cassandra.Client conn) throws Exception;
	}

	protected <ReturnType> ReturnType tryOperation(IManagerOperation<ReturnType> operation) throws Exception {

		openClient();
		try {
			// Execute operation
			ReturnType result = operation.execute(connection.getAPI());
			// Close client
			closeClient();
            // Return result!
			return result;
		} catch (Exception e) {
			closeClient();
			throw e;
		}
	}
}
