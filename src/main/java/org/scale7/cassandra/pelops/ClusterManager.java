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

import org.apache.cassandra.thrift.Cassandra.AsyncClient;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.describe_cluster_name_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.describe_version_call;
import org.apache.thrift.async.AsyncMethodCallback;

public class ClusterManager extends ManagerOperand {

	public ClusterManager(Cluster cluster) {
		super(cluster, null, 0);
	}

	/**
	 * Get the name of the cluster. Only nodes with the same cluster name communicate using the Gossip P2P protocol.
	 * @return					The name of the cluster
	 * @throws Exception
	 */
    public String getClusterName() throws Exception {
        IManagerOperation<describe_cluster_name_call, String> operation = new IManagerOperation<describe_cluster_name_call, String>() {

            @Override
            public void execute(AsyncClient conn,
                    AsyncMethodCallback<describe_cluster_name_call> callback)
                    throws Exception {
                conn.describe_cluster_name(callback);
            }

            @Override
            public String getResult(describe_cluster_name_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        return tryOperation(operation);
    }

    /**
     * Get the version of the Cassandra software being run by the cluster.
     * @return					The version of the Cassandra software
     * @throws Exception
     */
    public String getCassandraVersion() throws Exception {
        IManagerOperation<describe_version_call, String> operation = new IManagerOperation<describe_version_call, String>() {
            
            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<describe_version_call> callback)
                    throws Exception {
                conn.describe_version(callback);
            }

            @Override
            public String getResult(describe_version_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        return tryOperation(operation);
    }

}
