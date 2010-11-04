package org.scale7.cassandra.pelops.pool;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.support.AbstractIntegrationTest;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.sun.tools.internal.ws.wsdl.parser.Util.fail;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_TYPE_STANDARD;

/**
 * Tests the {@link CommonsBackedPool} class.
 */
public class CommonsBackedPoolIntegrationTest extends AbstractIntegrationTest {
    private static final String COLUMN_FAMILY = "CommonsBackedPoolCF";

    @BeforeClass
    public static void setup() throws Exception {
        AbstractIntegrationTest.setup(Arrays.asList(new CfDef(KEYSPACE, COLUMN_FAMILY)
                .setColumn_type(CFDEF_TYPE_STANDARD)
                .setComparator_type(CFDEF_COMPARATOR_BYTES)));
    }

    /**
     * Test that the background thread is disabled when a negative value is passed to the policy.
     */
    @Test
    public void testScheduledTasksThreadDisable() {
        CommonsBackedPool.Policy config = new CommonsBackedPool.Policy();
        config.setTimeBetweenScheduledTaskRunsMillis(-1); // disable the background thread

        CommonsBackedPool pool = null;
        try {
            pool = configurePool(config);

            for (Thread thread : getAllThreads()) {
                if (thread.getName().startsWith("pelops-pool-watcher-")) {
                    fail("Scheduled task thread appears to be running");
                }
            }
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Test that the background thread is started and stopped as appropriate.
     */
    @Test
    public void testScheduledTasksThread() {
        CommonsBackedPool.Policy config = new CommonsBackedPool.Policy();
        config.setTimeBetweenScheduledTaskRunsMillis(100);

        CommonsBackedPool pool = null;
        try {
            pool = configurePool(config);


            for (Thread thread : getAllThreads()) {
                if (thread.getName().startsWith("pelops-pool-watcher-")) {
                    return;
                }
            }
            fail("Scheduled task thread doesn't appears to be running");
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Test that the pool operates as expected when multiple threads are hitting it.
     */
    @Test
    public void testGetConnectionMultiThreaded() {
        CommonsBackedPool.Policy config = new CommonsBackedPool.Policy();
        config.setTimeBetweenScheduledTaskRunsMillis(-1); // disable the background thread
        config.setMaxActivePerNode(4); // one less than the number of worker threads

        final CommonsBackedPool pool = configurePool(config);
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(5);

            int taskCount = 1000;
            for (int i = 0; i < taskCount; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        Selector selector = pool.createSelector();
                        try {
                            selector.getColumnCount(COLUMN_FAMILY, "a", ConsistencyLevel.ONE);
                        } catch (Exception e) {
                            // do nothing
                        }
                    }
                });
            }
            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                fail("Failed to run all submitted tasks within a minute");
            }

            CommonsBackedPool.PooledNode node = pool.getPooledNode("localhost");

            assertEquals("Task count did not match connections borrowed", taskCount, pool.getStatistics().getConnectionsBorrowedTotal());
            assertEquals("Task count did not match connections borrowed on node", taskCount, node.getConnectionsBorrowedTotal());
            assertEquals("Task count did not match connections released", taskCount, pool.getStatistics().getConnectionsReleasedTotal());
            assertEquals("Task count did not match connections released on node", taskCount, node.getConnectionsReleasedTotal());
            assertEquals("Connections created did not match max active", config.getMaxActivePerNode(), pool.getStatistics().getConnectionsCreated());
            assertEquals("Connections created did not match max active on node", config.getMaxActivePerNode(), node.getConnectionsCreated());
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Test that a timeout exception is thrown when no connections are available.
     */
    @Test
    public void testTimeoutExceptionWhileWaitingOnConnection() throws Exception {
        CommonsBackedPool.Policy config = new CommonsBackedPool.Policy();
        config.setTimeBetweenScheduledTaskRunsMillis(-1); // disable the background thread
        config.setMaxActivePerNode(1);
        config.setMaxWaitForConnection(200); // 200 millis

        final CommonsBackedPool pool = configurePool(config);
        try {
            IThriftPool.IPooledConnection connection = pool.getConnection();
            try {
                pool.getConnection();
                fail("A connection was acquired when it shouldn't have been");
            } catch (TimeoutException e) {
                // expected
            }

            connection.release();
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Test that the connection is terminated when it's marked as corrupt.
     */
    @Test
    public void testConnectionTerminatedWhenCorrupt() throws Exception {
        CommonsBackedPool.Policy config = new CommonsBackedPool.Policy();
        config.setTimeBetweenScheduledTaskRunsMillis(-1); // disable the background thread
        config.setMaxActivePerNode(1);

        final CommonsBackedPool pool = configurePool(config);
        try {
            IThriftPool.IPooledConnection connection1 = pool.getConnection();
            connection1.corrupted();
            connection1.release();

            IThriftPool.IPooledConnection connection2 = pool.getConnection();
            assertFalse("The same corrupted exception was returned", connection1 == connection2);
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Test that when a node is suspended all it's connections are terminated and that when it comes good it starts
     * returning connections again.
     */
    @Test
    public void testsNodeSuspension() throws Exception {
        CommonsBackedPool.Policy config = new CommonsBackedPool.Policy();
        config.setTimeBetweenScheduledTaskRunsMillis(-1); // disable the background thread
        config.setMaxActivePerNode(1);

        final AtomicBoolean suspended = new AtomicBoolean(true);
        CommonsBackedPool pool = new CommonsBackedPool(
                AbstractIntegrationTest.cluster,
                config,
                new OperandPolicy(),
                AbstractIntegrationTest.KEYSPACE,
                new LeastLoadedNodeSelectionStrategy(),
                new CommonsBackedPool.INodeSuspensionStrategy() {
                    @Override
                    public boolean evaluate(CommonsBackedPool pool, CommonsBackedPool.PooledNode node) {
                        if (suspended.get()) {
                            // first run through we want to suspend the node
                            suspended.set(false);
                            node.setSuspensionState(new CommonsBackedPool.INodeSuspensionState() {
                                @Override
                                public boolean isSuspended() {
                                    return true;
                                }
                            });
                            return true;
                        } else {
                            // second run through we want the node active
                            node.setSuspensionState(new CommonsBackedPool.INodeSuspensionState() {
                                @Override
                                public boolean isSuspended() {
                                    return false;
                                }
                            });
                            return false;
                        }
                    }
                }
        );

        try {
            // node not yet suspended
            IThriftPool.IPooledConnection connection = pool.getConnection();
            connection.release();

            // suspend the node
            pool.runScheduledTasks();

            try {
                pool.getConnection();
                fail("No nodes should be available");
            } catch (TimeoutException e) {
                // expected
            }

            // activate the node
            pool.runScheduledTasks();

            // node is now active
            connection = pool.getConnection();
            connection.release();
        } finally {
            pool.shutdown();
        }
    }

    private CommonsBackedPool configurePool(CommonsBackedPool.Policy config) {
        return new CommonsBackedPool(
                AbstractIntegrationTest.cluster,
                config,
                new OperandPolicy(),
                AbstractIntegrationTest.KEYSPACE,
                new LeastLoadedNodeSelectionStrategy(),
                new NoOpNodeSuspensionStrategy()
        );
    }

    /*
        From http://nadeausoftware.com/articles/2008/04/java_tip_how_list_and_find_threads_and_thread_groups#Gettingalistofallthreads
     */
    private ThreadGroup getRootThreadGroup() {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        ThreadGroup ptg;
        while ((ptg = tg.getParent()) != null)
            tg = ptg;
        return tg;
    }

    private Thread[] getAllThreads() {
        final ThreadGroup root = getRootThreadGroup();
        final ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
        int nAlloc = thbean.getThreadCount();
        int n = 0;
        Thread[] threads;
        do {
            nAlloc *= 2;
            threads = new Thread[nAlloc];
            n = root.enumerate(threads, true);
        } while (n == nAlloc);
        return java.util.Arrays.copyOf(threads, n);
    }
}
